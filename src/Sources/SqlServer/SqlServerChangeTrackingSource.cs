﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
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
using Arcane.Framework.Sources.Exceptions;
using Arcane.Framework.Sources.Extensions;
using Microsoft.Data.SqlClient;
using Parquet.Data;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Sources.SqlServer;

/// <summary>
/// Akka Source for SQL Server table with change tracking enabled.
/// Supports max 600 columns if Azure Table Storage is used.
/// </summary>
public class SqlServerChangeTrackingSource : GraphStage<SourceShape<List<DataCell>>>, ITaggedSource, IParquetSource
{
    private readonly TimeSpan changeCaptureInterval;
    private readonly int commandTimeout;
    private readonly string connectionString;
    private readonly bool fullLoadOnstart;
    private readonly int lookBackRange;
    private readonly string schemaName;
    private readonly bool stopAfterFullLoad;
    private readonly string tableName;
    private readonly string datePartitionExpression;


    private SqlServerChangeTrackingSource(string connectionString, string schemaName, string tableName,
        TimeSpan changeCaptureInterval, int commandTimeout, int lookBackRange, bool fullLoadOnstart,
        bool stopAfterFullLoad, string datePartitionExpression = null)
    {
        this.connectionString = connectionString;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.changeCaptureInterval = changeCaptureInterval;
        this.commandTimeout = commandTimeout;
        this.lookBackRange = lookBackRange;
        this.fullLoadOnstart = fullLoadOnstart;
        this.stopAfterFullLoad = stopAfterFullLoad;
        this.datePartitionExpression = datePartitionExpression;
        this.Shape = new SourceShape<List<DataCell>>(this.Out);
    }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } =
        Attributes.CreateName(nameof(SqlServerChangeTrackingSource));

    /// <summary>
    /// Source outlet
    /// </summary>
    public Outlet<List<DataCell>> Out { get; } = new($"{nameof(SqlServerChangeTrackingSource)}.Out");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SourceShape<List<DataCell>> Shape { get; }


    /// <inheritdoc cref="IParquetSource.GetParquetSchema"/>
    public Schema GetParquetSchema()
    {
        using var sqlCon = new SqlConnection(this.connectionString);
        sqlCon.Open();
        var columnSummaries = SqlServerUtils.GetColumns(this.schemaName, this.tableName, sqlCon).ToList();
        var mergeExpression = SourceLogic.GetMergeExpression(columnSummaries, "tq");
        var matchExpression = SourceLogic.GetMatchStatement(columnSummaries, "tq", "ct");
        var columnExpression = SourceLogic.GetChangeTrackingColumns(columnSummaries,
            tableAlias: "tq", changesAlias: "ct");

        using var command = new SqlCommand(this.GetChangesQuery(
            mergeExpression,
            columnExpression,
            matchExpression,
            long.MaxValue, this.datePartitionExpression), sqlCon);
        command.CommandTimeout = this.commandTimeout;

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
    /// <param name="connectionString">Connection string, including database name.</param>
    /// <param name="schemaName">Schema name for the target table.</param>
    /// <param name="tableName">Table name.</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="commandTimeout">Timeout for sql commands issued by this source.</param>
    /// <param name="lookBackRange">Timestamp to get minimum commit_ts from.</param>
    /// <param name="fullLoadOnStart">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterFullLoad">Set to true if stream should stop after full load is finished</param>
    /// <param name="partitioningExpression">Optional expression to use to generate <see cref="Constants.DATE_PARTITION_KEY"/></param>
    /// <returns></returns>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static SqlServerChangeTrackingSource Create(
        string connectionString,
        string schemaName,
        string tableName,
        string partitioningExpression = null,
        TimeSpan? changeCaptureInterval = null,
        int commandTimeout = 3600,
        int lookBackRange = 86400,
        bool fullLoadOnStart = false,
        bool stopAfterFullLoad = false)
    {
        if (stopAfterFullLoad && !fullLoadOnStart)
        {
            throw new ArgumentException(
                $"{nameof(fullLoadOnStart)} must be true if {nameof(stopAfterFullLoad)} is set to true");
        }

        return new SqlServerChangeTrackingSource(
            connectionString: connectionString,
            schemaName: schemaName,
            tableName: tableName,
            changeCaptureInterval: changeCaptureInterval.GetValueOrDefault(TimeSpan.FromSeconds(15)),
            commandTimeout: commandTimeout,
            lookBackRange: lookBackRange,
            fullLoadOnstart: fullLoadOnStart,
            stopAfterFullLoad: stopAfterFullLoad,
            datePartitionExpression: partitioningExpression);
    }

    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new SourceLogic(this);
    }

    private string GetChangesQuery(string mergeExpression, string columnStatement, string matchStatement,
        long changeTrackingId, string partitionExpression = null)
    {
        var sqlConBuilder = new SqlConnectionStringBuilder(this.connectionString);
        var baseQuery = string.IsNullOrEmpty(partitionExpression) switch
        {
            true => File
                .ReadAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Sources", "SqlServer", "SqlSnippets",
                    "GetSelectDeltaQuery.sql")),
            false => File
                .ReadAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Sources", "SqlServer", "SqlSnippets",
                    "GetSelectDeltaQuery_date_partitioned.sql"))
        };
        return baseQuery
            .Replace("{dbName}", sqlConBuilder.InitialCatalog)
            .Replace("{schema}", this.schemaName)
            .Replace("{tableName}", this.tableName)
            .Replace("{ChangeTrackingColumnsStatement}", columnStatement)
            .Replace("{ChangeTrackingMatchStatement}", matchStatement)
            .Replace("{MERGE_EXPRESSION}", mergeExpression)
            .Replace("{MERGE_KEY}", Constants.UPSERT_MERGE_KEY)
            .Replace("{DATE_PARTITION_EXPRESSION}", partitionExpression)
            .Replace("{DATE_PARTITION_KEY}", Constants.DATE_PARTITION_KEY)
            .Replace("{lastId}", changeTrackingId.ToString());
    }

    private string GetAllQuery(string mergeExpression, string columnStatement, string partitionExpression = null)
    {
        var sqlConBuilder = new SqlConnectionStringBuilder(this.connectionString);
        var baseQuery = string.IsNullOrEmpty(partitionExpression) switch
        {
            true => File
                .ReadAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Sources", "SqlServer", "SqlSnippets",
                    "GetSelectAllQuery.sql")),
            false => File
                .ReadAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Sources", "SqlServer", "SqlSnippets",
                    "GetSelectAllQuery_date_partitioned.sql"))
        };

        return baseQuery
            .Replace("{dbName}", sqlConBuilder.InitialCatalog)
            .Replace("{schema}", this.schemaName)
            .Replace("{tableName}", this.tableName)
            .Replace("{ChangeTrackingColumnsStatement}", columnStatement)
            .Replace("{MERGE_EXPRESSION}", mergeExpression)
            .Replace("{MERGE_KEY}", Constants.UPSERT_MERGE_KEY)
            .Replace("{DATE_PARTITION_EXPRESSION}", partitionExpression)
            .Replace("{DATE_PARTITION_KEY}", Constants.DATE_PARTITION_KEY);
    }

    private sealed class SourceLogic : PollingSourceLogic, IStopAfterBackfill
    {
        private const string TimerKey = "Source";
        private readonly LocalOnlyDecider decider;
        private readonly SqlServerChangeTrackingSource source;
        private readonly SqlConnection sqlConnection;
        private string columnExpression;
        private long currentVersion;
        private string matchExpression;
        private string mergeExpression;
        private SqlDataReader reader;
        private Action<Task<Option<List<DataCell>>>> recordsReceived;
        private List<(string columnName, bool isPrimaryKey)> tableColumns;
        private SqlCommand command;

        public SourceLogic(SqlServerChangeTrackingSource source) : base(source.changeCaptureInterval, source.Shape)
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

        /// <inheritdoc cref="IStopAfterBackfill.IsRunningInBackfillMode"/>
        public bool IsRunningInBackfillMode { get; set; }

        /// <inheritdoc cref="IStopAfterBackfill.StopAfterBackfill"/>
        public bool StopAfterBackfill => this.source.stopAfterFullLoad;

        private void Finish(Exception cause)
        {
            if (cause is not null && cause is not SubscriptionWithCancelException.NonFailureCancellation)
            {
                this.FailSource(cause);
            }

            try
            {
                this.reader.Close();
            }
            catch (Exception ex)
            {
                this.Log.Warning(ex, "Couldn't complete transaction - probably done already.");
            }

            try
            {
                this.sqlConnection.Close();
                this.sqlConnection.Dispose();
                this.command?.Dispose();
                this.reader?.Close();
            }
            catch (Exception ex)
            {
                this.Log.Warning(ex,
                    "Failed to dispose sqlConnection and sqlTransaction objects - application might be leaking memory if this occurs again.");
            }
        }

        private long? GetChangeTrackingVersion(long version)
        {
            var query = (version == 0) switch
            {
                true => $"SELECT MIN(commit_ts) FROM sys.dm_tran_commit_table WHERE commit_time > '{DateTime.UtcNow.AddSeconds(-1 * this.source.lookBackRange):yyyy-MM-dd HH:mm:ss.fff}'",
                false => $"SELECT MIN(commit_ts) FROM sys.dm_tran_commit_table WHERE commit_ts > {version}",
            };

            using var getVersionCommand = new SqlCommand(query, this.sqlConnection);
            getVersionCommand.CommandTimeout = this.source.commandTimeout;

            this.Log.Debug("Executing {command}", getVersionCommand.CommandText);

            var value = getVersionCommand.ExecuteScalar();

            return value == DBNull.Value ? null : (long?)value;
        }

        public static string GetMatchStatement(IEnumerable<(string columnName, bool isPrimaryKey)> tableColumns,
            string sourceAlias,
            string outputAlias, IEnumerable<string> partitionColumns = null)
        {
            var mainMatch = string.Join(
                " and ",
                tableColumns.Where(tc => tc.isPrimaryKey).Select(pk =>
                    $"{outputAlias}.[{pk.columnName}] = {sourceAlias}.[{pk.columnName}]")
            );

            if (partitionColumns == null)
            {
                return mainMatch;
            }

            var partitionMatch = string.Join(
                " and ",
                partitionColumns.Select(pc => $"{outputAlias}.[{pc}] = {sourceAlias}.[{pc}]")
            );

            return $"{mainMatch} and ({sourceAlias}.SYS_CHANGE_OPERATION == 'D' OR ({partitionMatch}))";
        }

        public static string GetMergeExpression(
            IReadOnlyCollection<(string columnName, bool isPrimaryKey)> tableColumns, string tableAlias)
        {
            return string.Join(" + '#' + ", tableColumns
                .Where(tc => tc.isPrimaryKey)
                .Select(tc => $"cast({tableAlias}.[{tc.columnName}] as nvarchar(128))")
            );
        }

        public static string GetChangeTrackingColumns(
            IReadOnlyCollection<(string columnName, bool isPrimaryKey)> tableColumns,
            string changesAlias, string tableAlias)
        {
            return string.Join(",\n", tableColumns
                .Where(mc => mc.isPrimaryKey)
                .Select(mc => $"{changesAlias}.[{mc.columnName}]")
                .Append($"{changesAlias}.SYS_CHANGE_VERSION")
                .Append($"{changesAlias}.SYS_CHANGE_OPERATION")
                .Concat(tableColumns
                    .Where(tc =>
                        !tc.isPrimaryKey &&
                        !new[] { "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION" }.Contains(tc.columnName))
                    .Select(tc => $"{tableAlias}.[{tc.columnName}]")));
        }

        private string GetChangeTrackingColumns(string tableAlias)
        {
            return string.Join(",\n", this.tableColumns
                .Where(mc => mc.isPrimaryKey)
                .Select(mc => $"{tableAlias}.[{mc.columnName}]")
                .Append("0 as SYS_CHANGE_VERSION")
                .Append("'I' as SYS_CHANGE_OPERATION")
                .Concat(this.tableColumns
                    .Where(tc =>
                        !tc.isPrimaryKey &&
                        !new[] { "SYS_CHANGE_VERSION", "SYS_CHANGE_OPERATION" }.Contains(tc.columnName))
                    .Select(tc => $"{tableAlias}.[{tc.columnName}]")));
        }

        private void FailSource(Exception ex)
        {
            // SELECT *
            // FROM master.dbo.sysmessages
            // where description like N'change tracking is not enabled%table%'
            // ------------------------------------
            // error severity	dlevel	description
            // 4998	 16	        0       Change tracking is not enabled on table '%.*ls'.
            // 22105 16	        0       Change tracking is not enabled on table '%.*ls'.
            if (ex is SqlException rootCause)
            {
                if (rootCause.Number is 4998 or 22105)
                {
                    this.FailStage(new SchemaMismatchException(rootCause));
                }
                else
                {
                    this.FailStage(rootCause);
                }
            }
            else
            {
                this.FailStage(ex);
            }
        }

        private void TryExecuteReader()
        {
            try
            {
                this.reader = this.command.ExecuteReader();
            }
            catch (Exception ex)
            {
                this.FailSource(ex);
            }
        }

        private void GetChanges()
        {
            var newVersion = this.GetChangeTrackingVersion(this.currentVersion);

            if (newVersion.HasValue)
            {
                this.Log.Info("Fetching rows for version {version}, entity {database}.{schema}.{table}",
                    newVersion.Value - 1, this.sqlConnection.Database, this.source.schemaName, this.source.tableName);
            }
            else
            {
                this.Log.Info("No updates for entity {database}.{schema}.{table} since version {currentVersion}.",
                    this.sqlConnection.Database, this.source.schemaName, this.source.tableName, this.currentVersion);
            }


            var query = this.source.GetChangesQuery(this.mergeExpression,
                this.columnExpression,
                this.matchExpression,
                newVersion.GetValueOrDefault(long.MaxValue) - 1, this.source.datePartitionExpression);
            this.command = new SqlCommand(query, this.sqlConnection)
            {
                CommandTimeout = this.source.commandTimeout
            };
            this.TryExecuteReader();

            if (this.reader.HasRows)
            // reset current version so it can be updated from the source
            {
                this.currentVersion = 0;
            }
        }

        private void OnRecordReceived(Task<Option<List<DataCell>>> readTask)
        {
            if (readTask.IsFaulted || readTask.IsCanceled)
            {
                switch (this.decider.Decide(readTask.Exception))
                {
                    case Directive.Stop:
                        this.Finish(readTask.Exception);
                        break;
                    default:
                        this.ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
                        break;
                }

                return;
            }

            // Current batch has ended, start a new one
            if (readTask.Result.IsEmpty)
            {
                this.reader.Close();
                this.command.Dispose();
                this.command = null;
                if (this.CompleteStageAfterFullLoad(this.Finish))
                {
                    return;
                }

                this.GetChanges();
                this.ScheduleOnce(TimerKey, this.ChangeCaptureInterval);
            }
            else
            {
                if (this.currentVersion == 0)
                {
                    this.currentVersion = (long)readTask.Result.Value
                        .Find(v => v.FieldName == "ChangeTrackingVersion").Value;
                }

                this.Emit(this.source.Out, readTask.Result.Value);
            }
        }

        public override void PreStart()
        {
            this.currentVersion = 0;
            this.recordsReceived = this.GetAsyncCallback<Task<Option<List<DataCell>>>>(this.OnRecordReceived);
            this.sqlConnection.Open();
            this.tableColumns = SqlServerUtils
                .GetColumns(this.source.schemaName, this.source.tableName, this.sqlConnection).ToList();
            this.mergeExpression = this.source.fullLoadOnstart
                ? GetMergeExpression(this.tableColumns, "tq")
                : GetMergeExpression(this.tableColumns, "ct");
            this.matchExpression = GetMatchStatement(this.tableColumns, "tq", "ct");
            this.columnExpression = GetChangeTrackingColumns(this.tableColumns, tableAlias: "tq",
                changesAlias: "ct");

            if (this.source.fullLoadOnstart)
            {
                this.Log.Info("Fetching all rows for the latest version of an entity {database}.{schema}.{table}",
                    this.sqlConnection.Database, this.source.schemaName, this.source.tableName);

                var query = this.source.GetAllQuery(this.mergeExpression, this.GetChangeTrackingColumns("tq"), this.source.datePartitionExpression);
                this.command = new SqlCommand(query, this.sqlConnection)
                {
                    CommandTimeout = this.source.commandTimeout
                };

                this.IsRunningInBackfillMode = true;
                this.TryExecuteReader();
            }
            else
            {
                this.GetChanges();
            }
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
