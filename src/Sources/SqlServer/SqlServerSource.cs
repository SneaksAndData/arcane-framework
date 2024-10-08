﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Framework.Contracts;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.Base;
using Microsoft.Data.SqlClient;
using Parquet.Data;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Sources.SqlServer;

/// <summary>
/// The simple Source for reading data from Azure / on-prem Sql Server database.
/// This source does not support backfill and incremental loading.
/// </summary>
public class SqlServerSource : GraphStage<SourceShape<List<DataCell>>>, IParquetSource, ITaggedSource
{
    private readonly int commandTimeout;
    private readonly string connectionString;
    private readonly string schemaName;
    private readonly string tableName;
    private readonly TimeSpan changeCaptureInterval;

    private SqlServerSource(string connectionString, string schemaName, string tableName, int commandTimeout,
        TimeSpan changeCaptureInterval)
    {
        this.connectionString = connectionString;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.commandTimeout = commandTimeout;
        this.changeCaptureInterval = changeCaptureInterval;

        this.Shape = new SourceShape<List<DataCell>>(this.Out);
    }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(SqlServerSource));

    /// <summary>
    /// Source outlet
    /// </summary>
    public Outlet<List<DataCell>> Out { get; } = new($"{nameof(SqlServerSource)}.Out");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SourceShape<List<DataCell>> Shape { get; }

    /// <inheritdoc cref="IParquetSource.GetParquetSchema"/>
    public Schema GetParquetSchema()
    {
        using var sqlCon = new SqlConnection(this.connectionString);
        sqlCon.Open();
        var command = new SqlCommand(this.GetQuery(), sqlCon) { CommandTimeout = this.commandTimeout };
        using var schemaReader = command.ExecuteReader(CommandBehavior.SchemaOnly);

        return schemaReader.ToParquetSchema();
    }

    /// <inheritdoc cref="ITaggedSource.GetDefaultTags"/>
    public SourceTags GetDefaultTags()
    {
        var sqlConBuilder = new SqlConnectionStringBuilder(this.connectionString);
        return new SourceTags
        {
            SourceLocation = sqlConBuilder.InitialCatalog,
            SourceEntity = $"{sqlConBuilder.InitialCatalog}.{this.schemaName}.{this.tableName}"
        };
    }

    /// <summary>
    /// Creates a <see cref="Source{TOut,TMat}"/> for the Azure / on-prem Sql Server.
    /// </summary>
    /// <param name="connectionString">Sql server connection string</param>
    /// <param name="schemaName">Sql server schema name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="commandTimeout">Sql server command execution timeout</param>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static SqlServerSource Create(string connectionString,
        string schemaName,
        string tableName,
        TimeSpan changeCaptureInterval,
        int commandTimeout = 3600)
        => new(connectionString, schemaName, tableName, commandTimeout, changeCaptureInterval);

    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new SourceLogic(this);

    private string GetQuery()
    {
        var sqlConBuilder = new SqlConnectionStringBuilder(this.connectionString);
        return $"SELECT * FROM [{sqlConBuilder.InitialCatalog}].[{this.schemaName}].[{this.tableName}]";
    }

    private sealed class SourceLogic : PollingSourceLogic
    {
        private const string TimerKey = "PollTimer";
        private readonly LocalOnlyDecider decider;
        private readonly SqlServerSource source;
        private readonly SqlConnection sqlConnection;
        private SqlDataReader reader;
        private Action<Task<Option<List<DataCell>>>> recordsReceived;

        public SourceLogic(SqlServerSource source) : base(source.changeCaptureInterval, source.Shape)
        {
            this.source = source;
            this.sqlConnection = new SqlConnection(this.source.connectionString);
            this.decider = Decider.From((ex) => ex.GetType().Name switch
            {
                nameof(TimeoutException) => Directive.Restart,
                _ => Directive.Stop
            });

            this.SetHandler(source.Out, this.PullReader, this.Finish);
        }

        private void Finish(Exception cause)
        {
            this.reader.Close();
            this.sqlConnection.Close();
            this.sqlConnection.Dispose();
            if (cause is not null && cause is not SubscriptionWithCancelException.NonFailureCancellation)
            {
                this.FailStage(cause);
            }
            else
            {
                this.CompleteStage();
            }
        }

        private void OnRecordReceived(Task<Option<List<DataCell>>> readTask)
        {
            if (readTask.IsFaulted || readTask.IsCanceled)
            {
                switch (this.decider.Decide(readTask.Exception))
                {
                    case Directive.Stop:
                        this.FailStage(readTask.Exception);
                        break;
                    default:
                        this.ScheduleOnce(TimerKey, this.ChangeCaptureInterval);
                        break;
                }

                return;
            }

            // No more records from Sql Server
            if (readTask.Result.IsEmpty)
            {
                this.Log.Info("Rescheduling polling timer with interval {interval}", this.ChangeCaptureInterval);
                this.reader.Close();
                this.GetRows();
                this.ScheduleOnce(TimerKey, this.ChangeCaptureInterval);
            }
            else
            {
                this.Emit(this.source.Out, readTask.Result.Value);
            }
        }

        public override void PreStart()
        {
            this.recordsReceived = this.GetAsyncCallback<Task<Option<List<DataCell>>>>(this.OnRecordReceived);
            this.sqlConnection.Open();
            this.GetRows();
        }

        private void GetRows()
        {
            var command = new SqlCommand(this.source.GetQuery(), this.sqlConnection)
            { CommandTimeout = this.source.commandTimeout };
            this.reader = command.ExecuteReader();
        }

        private void PullReader()
        {
            this.reader.ReadAsync().Map(result =>
            {
                if (result)
                {
                    return Enumerable.Range(0, this.reader.FieldCount)
                        .Select(ixCol => new DataCell(this.reader.GetName(ixCol), this.reader.GetFieldType(ixCol),
                            this.reader.GetValue(ixCol)))
                        .ToList()
                        .AsOption();
                }

                return Option<List<DataCell>>.None;
            }).ContinueWith(this.recordsReceived);
        }

        protected override void OnTimer(object timerKey)
        {
            this.PullReader();
        }
    }
}
