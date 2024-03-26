using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Framework.Metrics.Models;
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
    private readonly string streamKind;
    private readonly string tableName;

    private SqlServerSource(string connectionString, string schemaName, string tableName, string streamKind,
        int commandTimeout)
    {
        this.connectionString = connectionString;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.commandTimeout = commandTimeout;
        this.streamKind = streamKind;

        Shape = new SourceShape<List<DataCell>>(Out);
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
        using var sqlCon = new SqlConnection(connectionString);
        sqlCon.Open();
        var command = new SqlCommand(GetQuery(), sqlCon) { CommandTimeout = commandTimeout };
        using var schemaReader = command.ExecuteReader(CommandBehavior.SchemaOnly);

        return schemaReader.ToParquetSchema();
    }

    /// <inheritdoc cref="ITaggedSource.GetDefaultTags"/>
    public SourceTags GetDefaultTags()
    {
        var sqlConBuilder = new SqlConnectionStringBuilder(connectionString);
        return new SourceTags
        {
            StreamKind = streamKind,
            SourceLocation = sqlConBuilder.InitialCatalog,
            SourceEntity = $"{sqlConBuilder.InitialCatalog}.{schemaName}.{tableName}"
        };
    }

    /// <summary>
    /// Creates a <see cref="Source{TOut,TMat}"/> for the Azure / on-prem Sql Server.
    /// </summary>
    /// <param name="connectionString">Sql server connection string</param>
    /// <param name="schemaName">Sql server schema name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="streamKind">Stream kind</param>
    /// <param name="commandTimeout">Sql server command execution timeout</param>
    public static SqlServerSource Create(string connectionString, string schemaName, string tableName,
        string streamKind, int commandTimeout = 3600)
    {
        return new SqlServerSource(connectionString, schemaName, tableName, streamKind, commandTimeout);
    }

    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new SourceLogic(this);
    }

    private string GetQuery()
    {
        var sqlConBuilder = new SqlConnectionStringBuilder(connectionString);
        return $"SELECT * FROM [{sqlConBuilder.InitialCatalog}].[{schemaName}].[{tableName}]";
    }

    private sealed class SourceLogic : TimerGraphStageLogic
    {
        private const string TimerKey = "PollTimer";
        private readonly LocalOnlyDecider decider;
        private readonly SqlServerSource source;
        private readonly SqlConnection sqlConnection;
        private SqlDataReader reader;
        private Action<Task<Option<List<DataCell>>>> recordsReceived;

        public SourceLogic(SqlServerSource source) : base(source.Shape)
        {
            this.source = source;
            sqlConnection = new SqlConnection(this.source.connectionString);
            decider = Decider.From((ex) => ex.GetType().Name switch
            {
                nameof(ArgumentException) => Directive.Stop,
                nameof(ArgumentNullException) => Directive.Stop,
                nameof(InvalidOperationException) => Directive.Stop,
                nameof(SqlException) => Directive.Stop,
                nameof(ConfigurationErrorsException) => Directive.Stop,
                nameof(ObjectDisposedException) => Directive.Stop,
                nameof(IOException) => Directive.Stop,
                nameof(TimeoutException) => Directive.Restart,
                _ => Directive.Stop
            });

            SetHandler(source.Out, PullReader, Finish);
        }

        private void Finish(Exception cause)
        {
            reader.Close();
            sqlConnection.Close();
            sqlConnection.Dispose();
            if (cause is not null && cause is not SubscriptionWithCancelException.NonFailureCancellation)
                FailStage(cause);
            else
                CompleteStage();
        }

        private void OnRecordReceived(Task<Option<List<DataCell>>> readTask)
        {
            if (readTask.IsFaulted || readTask.IsCanceled)
            {
                switch (decider.Decide(readTask.Exception))
                {
                    case Directive.Stop:
                        FailStage(readTask.Exception);
                        break;
                    default:
                        ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
                        break;
                }

                return;
            }

            // No more records from Sql Server
            if (readTask.Result.IsEmpty)
                Finish(null);
            else
                Emit(source.Out, readTask.Result.Value);
        }

        public override void PreStart()
        {
            recordsReceived = GetAsyncCallback<Task<Option<List<DataCell>>>>(OnRecordReceived);
            sqlConnection.Open();
            var command = new SqlCommand(source.GetQuery(), sqlConnection) { CommandTimeout = source.commandTimeout };
            reader = command.ExecuteReader();
        }

        private void PullReader()
        {
            reader.ReadAsync().Map(result =>
            {
                if (result)
                    return Enumerable.Range(0, reader.FieldCount)
                        .Select(ixCol => new DataCell(reader.GetName(ixCol), reader.GetFieldType(ixCol),
                            reader.GetValue(ixCol)))
                        .ToList()
                        .AsOption();

                return Option<List<DataCell>>.None;
            }).ContinueWith(recordsReceived);
        }

        protected override void OnTimer(object timerKey)
        {
            PullReader();
        }
    }
}
