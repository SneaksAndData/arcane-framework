using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text.Json;
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
using Arcane.Framework.Sources.CdmChangeFeedSource.Exceptions;
using Arcane.Framework.Sources.CdmChangeFeedSource.Extensions;
using Arcane.Framework.Sources.CdmChangeFeedSource.Models;
using Arcane.Framework.Sources.Exceptions;
using Arcane.Framework.Sources.Extensions;
using Parquet.Data;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;

namespace Arcane.Framework.Sources.CdmChangeFeedSource;

/// <summary>
/// Akka Source for Microsoft Common Data Model (CDM) change feed.
/// </summary>
public class CdmChangeFeedSource : GraphStage<SourceShape<List<DataCell>>>, IParquetSource, ITaggedSource
{
    private const string mergeColumnName = "RECID";
    private readonly IBlobStorageService blobStorage;
    private readonly TimeSpan changeCaptureInterval;
    private readonly string entityName;
    private readonly bool isBackfilling;
    private readonly string rootPath;
    private readonly TimeSpan schemaUpdateInterval;
    private readonly bool stopAfterBackfill;
    private readonly int lookBackRange;

    private CdmChangeFeedSource(string rootPath,
        string entityName,
        IBlobStorageService blobStorage,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        bool stopAfterBackfill,
        TimeSpan schemaUpdateInterval,
        int lookBackRange
        )
    {
        this.rootPath = rootPath;
        this.entityName = entityName;
        this.blobStorage = blobStorage;
        this.isBackfilling = isBackfilling;
        this.changeCaptureInterval = changeCaptureInterval;
        this.stopAfterBackfill = stopAfterBackfill;
        this.Shape = new SourceShape<List<DataCell>>(this.Out);
        this.schemaUpdateInterval = schemaUpdateInterval;
        this.lookBackRange = lookBackRange;
    }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(CdmChangeFeedSource));

    /// <summary>
    /// Source outlet
    /// </summary>
    public Outlet<List<DataCell>> Out { get; } = new($"{nameof(CdmChangeFeedSource)}.Out");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SourceShape<List<DataCell>> Shape { get; }

    /// <inheritdoc cref="IParquetSource.GetParquetSchema"/>
    public Schema GetParquetSchema()
    {
        return this.GetCdmSchema().GetReader(Constants.UPSERT_MERGE_KEY).ToParquetSchema();
    }

    /// <inheritdoc cref="ITaggedSource.GetDefaultTags"/>
    public SourceTags GetDefaultTags()
    {
        return new SourceTags
        {
            SourceEntity = this.entityName,
            SourceLocation = this.rootPath,
        };
    }

    /// <summary>
    /// Creates a <see cref="Source{TOut,TMat}"/> for Microsoft Common Data Model (CDM) change feed.
    /// </summary>
    /// <param name="rootPath">The root path of the CDM entities locations</param>
    /// <param name="entityName">Name of the entity being streamed.</param>
    /// <param name="blobStorage">Blob storage service</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="schemaUpdateInterval">Interval to refresh schema.</param>
    /// <param name="lookBackRange">Timestamp to get minimum commit_ts from.</param>
    /// <param name="isBackfilling">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterBackfill">Set to true if stream should stop after full load is finished</param>
    /// <returns></returns>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static CdmChangeFeedSource Create(string rootPath,
        string entityName,
        IBlobStorageService blobStorage,
        bool isBackfilling = true,
        TimeSpan? changeCaptureInterval = null,
        bool stopAfterBackfill = false,
        int lookBackRange = 86400,
        TimeSpan? schemaUpdateInterval = null)
    {
        if (!isBackfilling && stopAfterBackfill)
        {
            throw new ArgumentException(
                $"{nameof(isBackfilling)} must be true if {nameof(stopAfterBackfill)} is set to true");
        }

        return new CdmChangeFeedSource(rootPath,
            entityName,
            blobStorage,
            isBackfilling,
            changeCaptureInterval ?? TimeSpan.FromSeconds(15),
            stopAfterBackfill,
            schemaUpdateInterval ?? TimeSpan.FromSeconds(60),
            lookBackRange);
    }

    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new SourceLogic(this);
    }

    private SimpleCdmEntity GetCdmSchema()
    {
        var schemaPath = $"ChangeFeed/{this.entityName}.cdm.json";
        var schemaData = this.blobStorage.GetBlobContent(this.rootPath, schemaPath,
            bd => JsonSerializer.Deserialize<JsonDocument>(bd.ToString()));

        return SimpleCdmEntity.FromJson(schemaData);
    }

    private sealed class SourceLogic : PollingSourceLogic, IStopAfterBackfill
    {
        private const string TimerKey = "Source";
        private readonly string changeFeedPath;
        private readonly LocalOnlyDecider decider;
        private readonly CdmChangeFeedSource source;
        private readonly string tablesPath;
        private SimpleCdmEntity changeFeedSchema;
        private IEnumerable<List<DataCell>> changes;
        private DateTimeOffset lastProcessedTimestamp;
        private DateTimeOffset? maxAvailableTimestamp;
        private DateTimeOffset? nextSchemaUpdateTimestamp;

        public SourceLogic(CdmChangeFeedSource source) : base(source.changeCaptureInterval, source.Shape)
        {
            this.source = source;
            this.changeFeedPath = $"{source.rootPath}/ChangeFeed/{source.entityName}";
            this.tablesPath = $"{source.rootPath}/Tables";
            this.lastProcessedTimestamp = DateTimeOffset.UtcNow.AddSeconds(-this.source.lookBackRange);

            this.decider = Decider.From((ex) => ex.GetType().Name switch
            {
                nameof(TimeoutException) => Directive.Restart,
                _ => Directive.Stop
            });


            this.SetHandler(source.Out, this.PullChanges);
        }

        /// <inheritdoc cref="IStopAfterBackfill.IsRunningInBackfillMode"/>
        public bool IsRunningInBackfillMode { get; set; }

        /// <inheritdoc cref="IStopAfterBackfill.StopAfterBackfill"/>
        public bool StopAfterBackfill => this.source.stopAfterBackfill;

        public override void PreStart()
        {
            this.UpdateSchema(false);
            if (this.source.isBackfilling)
            {
                this.PrepareEntityAsChanges();
            }
            else
            {
                this.PrepareChanges();
            }
        }

        private void DecideOnFailure(Exception ex)
        {
            switch (this.decider.Decide(ex))
            {
                case Directive.Stop:
                    // wrap exception in schema change if we catch out of range on csv parse
                    if (ex is IndexOutOfRangeException)
                    {
                        ex = new SchemaMismatchException(ex);
                    }

                    this.FailStage(ex);
                    break;
                default:
                    this.ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
                    break;
            }
        }

        private (Type, object) ConvertToCdmType(string cdmDataType, string value)
        {
            var tp = SimpleCdmAttribute.MapCdmType(cdmDataType);
            var converter = TypeDescriptor.GetConverter(tp);
            return (tp, value == null ? null : converter.ConvertFromInvariantString(value));
        }

        private IEnumerable<List<DataCell>> ProcessEntityBlob(StoredBlob blob,
            Dictionary<string, int> fieldSortIndexes)
        {
            var blobSchemaPath = string.Join("/",
                blob.Name.Split("/")[1..^2].Append($"{this.source.entityName}.cdm.json"));
            var blobSchema = SimpleCdmEntity.FromJson(this.source.blobStorage.GetBlobContent(this.source.rootPath,
                blobSchemaPath, (bd) => JsonSerializer.Deserialize<JsonDocument>(bd.ToString())));
            var blobStream = this.source.blobStorage.StreamBlobContent(this.source.rootPath,
                string.Join("/", blob.Name.Split("/")[1..]));
            using StreamReader sr = new(blobStream);
            while (!sr.EndOfStream)
            {
                var csvLine = sr.ReadLine();

                while (!CsvOperations.IsComplete(csvLine))
                {
                    csvLine += sr.ReadLine();
                }

                // need to align base entity with change feed schema
                // first we parse entity line using entity schema and then modify the resulting DataCell collection to fit change feed schema
                var cells = CsvOperations
                    .ParseCsvLine(csvLine, blobSchema.Attributes.Length)
                    .Select((v, ix) =>
                    {
                        var (tp, value) = this.ConvertToCdmType(blobSchema.Attributes[ix].DataType, v);
                        var fieldName = blobSchema.Attributes[ix].Name == "LSN"
                            ? "Start_LSN"
                            : blobSchema.Attributes[ix].Name;
                        return new DataCell(fieldName, tp, value);
                    })
                    .Where(dc =>
                        dc.FieldName !=
                        "_SysRowId") // exclude system field as it is not present in the change feed
                    // append fields from the change feed schema with default values
                    .Append(new DataCell("End_LSN", typeof(string), null))
                    .Append(new DataCell("DML_Action", typeof(string), "INSERT"))
                    .Append(new DataCell("Seq_Val", typeof(string),
                        "0x00000000000000000000"))
                    .Append(new DataCell("Update_Mask", typeof(string),
                        "0x00000000000000000000"))
                    .OrderBy(cell => fieldSortIndexes[cell.FieldName])
                    .ToList();

                var mergeKeyCell =
                    cells.FirstOrDefault(cell => cell.FieldName == mergeColumnName) ??
                    throw new MissingPrimaryKeyException(
                        $"Missing {mergeColumnName} in the incoming csv line: {csvLine}");

                cells.Add(new DataCell(Constants.UPSERT_MERGE_KEY, typeof(string),
                    mergeKeyCell.Value.ToString()));

                yield return cells;
            }
        }

        private void PrepareEntityAsChanges()
        {
            try
            {
                var tableBlobs = this.source.blobStorage.ListBlobsAsEnumerable(this.tablesPath).Where(blob =>
                    blob.Name.Split("/")[^1].StartsWith($"{this.source.entityName.ToUpper()}_") &&
                    blob.Name.EndsWith(".csv")).ToList();
                var sortIndexes = this.changeFeedSchema.Attributes.Select((attr, ix) => (attr.Name, ix))
                    .ToDictionary(v => v.Name, v => v.ix);
                this.changes = tableBlobs.SelectMany(blob => this.ProcessEntityBlob(blob, sortIndexes));
                this.IsRunningInBackfillMode = true;
            }
            catch (Exception ex)
            {
                this.DecideOnFailure(ex);
            }
        }

        private void PrepareChanges()
        {
            var blobList = this.source.blobStorage.ListBlobsAsEnumerable(this.changeFeedPath)
                .Where(blob => blob.LastModified > this.lastProcessedTimestamp && blob.Name.EndsWith(".csv")).ToList();

            this.maxAvailableTimestamp = blobList.Select(b => b.LastModified).Max();
            this.changes = blobList
                .Select(blob =>
                {
                    try
                    {
                        var fileContents = this.source.blobStorage.GetBlobContent(this.changeFeedPath,
                            blob.Name.Split("/")[^1], (bd) => bd.ToString());
                        return CsvOperations.ReplaceQuotedNewlines(fileContents).Split("\n").AsOption();
                    }
                    catch (OutOfMemoryException exception)
                    {
                        this.Log.Error("Failed to read file {cdmFile} due to {exception}", blob.Name, exception);
                        throw;
                    }
                    catch (Exception ex)
                    {
                        this.Log.Warning(ex, "Failed file read, {cdmFile}", blob.Name);
                        return Option<string[]>.None;
                    }
                })
                .Where(v => v.HasValue)
                .SelectMany(v => v.Value)
                .Where(line => !string.IsNullOrEmpty(line))
                .Select(line =>
                {
                    var cells = CsvOperations.ParseCsvLine(line, this.changeFeedSchema.Attributes.Length).Select(
                        (v, ix) =>
                        {
                            var (tp, value) = this.ConvertToCdmType(this.changeFeedSchema.Attributes[ix].DataType, v);
                            return new DataCell(this.changeFeedSchema.Attributes[ix].Name, tp,
                                value);
                        }).ToList();

                    var mergeKeyCell =
                        cells.FirstOrDefault(cell => cell.FieldName == mergeColumnName) ??
                        throw new MissingPrimaryKeyException(
                            $"Missing {mergeColumnName} in the incoming csv line: {line}");

                    cells.Add(new DataCell(Constants.UPSERT_MERGE_KEY, typeof(string),
                        mergeKeyCell.Value.ToString()));

                    return cells;
                });
        }

        private void PullChanges()
        {
            if (DateTimeOffset.Now > this.nextSchemaUpdateTimestamp)
            {
                this.UpdateSchema(true);
            }

            if (!this.changes.Any())
            {
                if (!this.CompleteStageAfterFullLoad())
                {
                    this.ScheduleOnce(TimerKey, this.ChangeCaptureInterval);
                }
            }
            else
            {
                this.lastProcessedTimestamp = this.maxAvailableTimestamp.HasValue && this.maxAvailableTimestamp > this.lastProcessedTimestamp
                    ? this.maxAvailableTimestamp.Value
                    : this.lastProcessedTimestamp;
                this.EmitMultiple(this.source.Out, this.changes);

                this.changes = Enumerable.Empty<List<DataCell>>();
            }
        }

        private void UpdateSchema(bool allowMissingSchema)
        {
            var changeFeedSchemaPath = $"ChangeFeed/{this.source.entityName}.cdm.json";
            var schemaContent = this.source
                .blobStorage
                .GetBlobContent(this.source.rootPath, changeFeedSchemaPath, bd
                    => (bd, allowMissingSchema) switch
                    {
                        (null, true) => null,
                        (null, false) => throw new SchemaNotFoundException(this.source.entityName),
                        var (data, _) => JsonSerializer.Deserialize<JsonDocument>(data.ToString())
                    }
                );
            if (schemaContent == null)
            {
                this.Log.Warning("Could not update schema");
            }
            else
            {
                var newSchema = SimpleCdmEntity.FromJson(schemaContent);
                var schemaUnchanged =
                    SimpleCdmEntity.SimpleCdmEntityComparer.Equals(this.changeFeedSchema, newSchema);
                this.changeFeedSchema = (this.changeFeedSchema == null, schemaEquals: schemaUnchanged) switch
                {
                    (true, _) or (false, true) => newSchema,
                    (false, false) => throw new SchemaMismatchException()
                };
            }

            this.nextSchemaUpdateTimestamp = DateTimeOffset.Now + this.source.schemaUpdateInterval;
        }

        protected override void OnTimer(object timerKey)
        {
            try
            {
                this.PrepareChanges();
                this.PullChanges();
            }
            catch (Exception ex)
            {
                this.DecideOnFailure(ex);
            }
        }
    }
}
