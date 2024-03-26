using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Stage;
using Arcane.Framework.Sinks.Parquet;
using Parquet;
using Parquet.Data;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Snd.Sdk.Tasks;
using ParquetColumn = Parquet.Data.DataColumn;

namespace Arcane.Framework.Sinks;

/// <summary>
/// Sink that allows writing data to Azure blob in Parquet format.
/// </summary>
public class ParquetSink : GraphStageWithMaterializedValue<SinkShape<List<ParquetColumn>>, Task>
{
    private readonly bool createSchemaFile;
    private readonly string dataSinkPathSegment;
    private readonly bool dropCompletionToken;
    private readonly Schema parquetSchema;
    private readonly bool partitionByDate;
    private readonly string path;
    private readonly int rowGroupsPerFile;
    private readonly string schemaSinkPathSegment;
    private readonly IBlobStorageWriter storageWriter;

    /// <summary>
    /// Creates a new instance of <see cref="ParquetSink"/>
    /// </summary>
    private ParquetSink(Schema parquetSchema, IBlobStorageWriter storageWriter, string parquetFilePath,
        int rowGroupsPerFile, bool createSchemaFile, bool partitionByDate, string dataSinkPathSegment,
        string schemaSinkPathSegment, bool dropCompletionToken)
    {
        this.parquetSchema = parquetSchema;
        this.storageWriter = storageWriter;
        path = parquetFilePath;
        this.rowGroupsPerFile = rowGroupsPerFile == 0
            ? throw new ArgumentException(
                $"{nameof(rowGroupsPerFile)} should be greater then 0, but was {rowGroupsPerFile}")
            : rowGroupsPerFile;
        this.createSchemaFile = createSchemaFile;
        this.partitionByDate = partitionByDate;
        this.dataSinkPathSegment = dataSinkPathSegment;
        this.schemaSinkPathSegment = schemaSinkPathSegment;
        this.dropCompletionToken = dropCompletionToken;

        Shape = new SinkShape<List<ParquetColumn>>(In);
    }

    /// <summary>
    /// Sink outlet
    /// </summary>
    public Inlet<List<ParquetColumn>> In { get; } = new($"{nameof(ParquetSink)}.In");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SinkShape<List<ParquetColumn>> Shape { get; }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(ParquetSink));

    /// <summary>
    /// Creates a new instance of <see cref="ParquetSink"/>
    /// </summary>
    /// <param name="parquetSchema">Parquet schema</param>
    /// <param name="storageWriter">Storage writer service</param>
    /// <param name="parquetFilePath">File path</param>
    /// <param name="rowGroupsPerFile">Number of row groups per each file</param>
    /// <param name="createSchemaFile">True if sink should drop schema file before emitting the data</param>
    /// <param name="partitionByDate">True if sink should partition output by date</param>
    /// <param name="dataSinkPathSegment">Folder name to emit data</param>
    /// <param name="schemaSinkPathSegment">Folder name to emit schema</param>
    /// <param name="dropCompletionToken">True if sink should drop a file when complete.</param>
    /// <returns></returns>
    public static ParquetSink Create(Schema parquetSchema, IBlobStorageWriter storageWriter, string parquetFilePath,
        int rowGroupsPerFile = 1, bool createSchemaFile = false, bool partitionByDate = false,
        string dataSinkPathSegment = "data", string schemaSinkPathSegment = "schema", bool dropCompletionToken = false)
    {
        return new ParquetSink(parquetSchema, storageWriter, parquetFilePath, rowGroupsPerFile, createSchemaFile,
            partitionByDate, dataSinkPathSegment, schemaSinkPathSegment, dropCompletionToken);
    }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.CreateLogicAndMaterializedValue"/>
    public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
    {
        var completion = new TaskCompletionSource<NotUsed>();
        return new LogicAndMaterializedValue<Task>(new SinkLogic(this, completion),
            completion.Task);
    }

    private sealed class SinkLogic : GraphStageLogic
    {
        private readonly LocalOnlyDecider decider;
        private readonly ParquetSink sink;
        private readonly TaskCompletionSource<NotUsed> taskCompletion;
        private int blockCount;
        private MemoryStream memoryStream;
        private string schemaHash;

        private bool writeInProgress;

        public SinkLogic(ParquetSink sink, TaskCompletionSource<NotUsed> taskCompletion) :
            base(sink.Shape)
        {
            this.sink = sink;
            this.taskCompletion = taskCompletion;
            decider = Decider.From((ex) => ex.GetType().Name switch
            {
                nameof(ArgumentException) => Directive.Stop,
                nameof(ArgumentNullException) => Directive.Stop,
                _ => Directive.Stop
            });
            writeInProgress = false;

            SetHandler(sink.In,
                () => WriteRowGroup(Grab(sink.In)),
                () =>
                {
                    // It is most likely that we receive the finish event before the task from the last element has finished
                    // so if the task is still running we need to complete the stage later
                    if (!writeInProgress)
                        Finish();
                },
                ex =>
                {
                    this.taskCompletion.TrySetException(ex);
                    FailStage(ex);
                }
            );
        }

        public override void PreStart()
        {
            // Keep going even if the upstream has finished so that we can process the task from the last element
            SetKeepGoing(true);

            if (sink.createSchemaFile)
                // dump empty schema file and then pull first element
                CreateSchemaFile().ContinueWith(_ => GetAsyncCallback(() => Pull(sink.In)).Invoke());
            else
                // request the first element
                Pull(sink.In);
        }

        private string GetSavePath()
        {
            var basePath = sink.partitionByDate
                ? $"{sink.path}/_batch_date={DateTimeOffset.UtcNow:yyyy-MM-dd}"
                : sink.path;
            return $"{basePath}/{sink.dataSinkPathSegment}";
        }

        private string GetSchemaPath()
        {
            return $"{sink.path}/{sink.schemaSinkPathSegment}";
        }

        private Task<UploadedBlob> CreateSchemaFile()
        {
            var (fullHash, shortHash, schemaBytes) = sink.parquetSchema.GetSchemaHash();
            schemaHash = shortHash;
            Log.Info("Schema hash length for this source: {schemaByteLength}", schemaBytes.Length);
            Log.Info("Full schema hash for this source: {schemaHash}", fullHash);

            var schemaId = Guid.NewGuid();
            // Save empty file to base output location and schema store
            return sink.storageWriter.SaveBytesAsBlob(new BinaryData(schemaBytes), GetSavePath(),
                    $"part-{schemaId}-{schemaHash}-chunk.parquet")
                .Map(_ => sink.storageWriter.SaveBytesAsBlob(new BinaryData(schemaBytes), GetSchemaPath(),
                    $"schema-{schemaId}-{schemaHash}.parquet"))
                .Flatten();
        }

        private Task<UploadedBlob> SavePart()
        {
            return sink.storageWriter.SaveBytesAsBlob(new BinaryData(memoryStream.ToArray()), GetSavePath(),
                string.IsNullOrEmpty(schemaHash)
                    ? $"part-{Guid.NewGuid()}-chunk.parquet"
                    : $"part-{Guid.NewGuid()}-{schemaHash}-chunk.parquet");
        }

        private Task<UploadedBlob> SaveCompletionToken()
        {
            if (sink.dropCompletionToken)
                // there seems to be an issue with Moq library and how it serializes BinaryData type
                // in order to have consistent behaviour between units and actual runs we write byte 0 to the file
                return sink.storageWriter.SaveBytesAsBlob(new BinaryData(new byte[] { 0 }), GetSavePath(),
                    $"{schemaHash}.COMPLETED");

            return Task.FromResult(new UploadedBlob());
        }

        private void WriteRowGroup(List<ParquetColumn> parquetColumns)
        {
            if (parquetColumns.Count == 0)
            {
                Log.Info("Received and empty chunk for {sinkPath}", sink.path);
                return;
            }

            if (blockCount == 0) memoryStream = new MemoryStream();

            writeInProgress = true;
            blockCount += 1;
            Log.Info("Processing inlet {blockCount} for {sinkPath}, size {dataLength}", blockCount, sink.path,
                parquetColumns[0].Data.Length);
            try
            {
                using (var parquetWriter = new ParquetWriter(sink.parquetSchema, memoryStream,
                           append: sink.rowGroupsPerFile > 1 && blockCount > 1))
                {
                    using (var groupWriter = parquetWriter.CreateRowGroup())
                    {
                        foreach (var parquetColumn in parquetColumns) groupWriter.WriteColumn(parquetColumn);
                    }
                }

                if (blockCount % sink.rowGroupsPerFile == 0 || IsClosed(sink.In))
                {
                    SavePart().ContinueWith(_ => GetAsyncCallback(PullOrComplete).Invoke());
                    blockCount = 0;
                }
                else
                {
                    PullOrComplete();
                }
            }
            catch (Exception ex)
            {
                switch (decider.Decide(ex))
                {
                    case Directive.Stop:
                        taskCompletion.TrySetException(ex);
                        FailStage(ex);
                        break;
                    case Directive.Resume:
                        WriteRowGroup(parquetColumns);
                        break;
                    case Directive.Restart:
                        PullOrComplete();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        private void CompleteSink()
        {
            taskCompletion.TrySetResult(NotUsed.Instance);
            CompleteStage();
        }

        private void Finish()
        {
            if (memoryStream is { Length: > 0 })
                SavePart().Map(_ => SaveCompletionToken()).Flatten()
                    .ContinueWith(_ => GetAsyncCallback(CompleteSink).Invoke());
            else
                SaveCompletionToken().ContinueWith(_ => GetAsyncCallback(CompleteSink).Invoke());
        }

        private void PullOrComplete()
        {
            writeInProgress = false;
            if (IsClosed(sink.In))
                Finish();
            else
                Pull(sink.In);
        }
    }
}
