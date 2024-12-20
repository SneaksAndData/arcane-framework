﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Arcane.Framework.Sinks.Extensions;
using Arcane.Framework.Sinks.Models;
using Arcane.Framework.Sinks.Services.Base;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Framework.Services.Base;
using Parquet;
using Parquet.Data;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Snd.Sdk.Tasks;
using ParquetColumn = Parquet.Data.DataColumn;

namespace Arcane.Framework.Sinks.Parquet;

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
    private readonly string metadataSinkPathSegment;
    private readonly IBlobStorageWriter storageWriter;
    private readonly StreamMetadata metadata;
    private readonly IInterruptionToken interruptionToken;

    /// <summary>
    /// Creates a new instance of <see cref="ParquetSink"/>
    /// </summary>
    private ParquetSink(Schema parquetSchema,
        IBlobStorageWriter storageWriter,
        string parquetFilePath,
        int rowGroupsPerFile,
        bool createSchemaFile,
        bool partitionByDate,
        string dataSinkPathSegment,
        string schemaSinkPathSegment,
        bool dropCompletionToken,
        StreamMetadata streamMetadata,
        string metadataSinkPathSegment,
        IInterruptionToken interruptionToken)
    {
        this.parquetSchema = parquetSchema;
        this.storageWriter = storageWriter;
        this.path = parquetFilePath;
        this.rowGroupsPerFile = rowGroupsPerFile == 0
            ? throw new ArgumentException(
                $"{nameof(rowGroupsPerFile)} should be greater then 0, but was {rowGroupsPerFile}")
            : rowGroupsPerFile;
        this.createSchemaFile = createSchemaFile;
        this.partitionByDate = partitionByDate;
        this.dataSinkPathSegment = dataSinkPathSegment;
        this.schemaSinkPathSegment = schemaSinkPathSegment;
        this.metadataSinkPathSegment = metadataSinkPathSegment;
        this.dropCompletionToken = dropCompletionToken;
        this.metadata = streamMetadata;
        this.interruptionToken = interruptionToken;

        this.Shape = new SinkShape<List<ParquetColumn>>(this.In);
    }

    /// <summary>
    /// Sink inlet
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
    /// <param name="streamMetadata">Metadata that describes data produced by the stream</param>
    /// <param name="metadataSinkPathSegment">Folder name to emit metadata</param>
    /// <param name="interruptionToken">Provides information about streaming container interrupteion</param>
    /// <returns></returns>
    public static ParquetSink Create(Schema parquetSchema,
        IBlobStorageWriter storageWriter,
        IInterruptionToken interruptionToken,
        string parquetFilePath,
        StreamMetadata streamMetadata,
        int rowGroupsPerFile = 1,
        bool createSchemaFile = false,
        bool partitionByDate = false,
        string dataSinkPathSegment = "data",
        string schemaSinkPathSegment = "schema",
        bool dropCompletionToken = false,
        string metadataSinkPathSegment = "metadata")
    {
        return new ParquetSink(parquetSchema, storageWriter, parquetFilePath, rowGroupsPerFile, createSchemaFile,
            partitionByDate, dataSinkPathSegment, schemaSinkPathSegment, dropCompletionToken, streamMetadata,
            metadataSinkPathSegment, interruptionToken);
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
        private readonly IMetadataWriter metadataWriter;
        private readonly IInterruptionToken interruptionToken;

        public SinkLogic(ParquetSink sink, TaskCompletionSource<NotUsed> taskCompletion) :
            base(sink.Shape)
        {
            this.sink = sink;
            this.taskCompletion = taskCompletion;
            this.metadataWriter = sink.metadata.ToStreamMetadataWriter(this.sink.storageWriter, this.GetMetadataPath());
            this.decider = Decider.From((ex) => ex.GetType().Name switch
            {
                nameof(ArgumentException) => Directive.Stop,
                nameof(ArgumentNullException) => Directive.Stop,
                _ => Directive.Stop
            });
            this.writeInProgress = false;
            this.interruptionToken = sink.interruptionToken;


            this.SetHandler(sink.In,
                () => this.WriteRowGroup(this.Grab(sink.In)),
                () =>
                {
                    // It is most likely that we receive the finish event before the task from the last element has finished
                    // so if the task is still running we need to complete the stage later
                    if (!this.writeInProgress)
                    {
                        this.Finish();
                    }
                },
                ex =>
                {
                    this.taskCompletion.TrySetException(ex);
                    this.FailStage(ex);
                }
            );
        }

        public override void PreStart()
        {
            // Keep going even if the upstream has finished so that we can process the task from the last element
            this.SetKeepGoing(true);

            this.metadataWriter.Write(this.Log).GetAwaiter().GetResult();
            if (this.sink.createSchemaFile)
                // dump empty schema file and then pull first element
            {
                this.CreateSchemaFile()
                    .ContinueWith(_ => this.GetAsyncCallback(() => this.Pull(this.sink.In)).Invoke());
            }
            else
                // request the first element
            {
                this.Pull(this.sink.In);
            }
        }

        private string GetSavePath()
        {
            var basePath = this.sink.partitionByDate
                ? $"{this.sink.path}/_batch_date={DateTimeOffset.UtcNow:yyyy-MM-dd}"
                : this.sink.path;
            return $"{basePath}/{this.sink.dataSinkPathSegment}";
        }

        private string GetSchemaPath()
        {
            return $"{this.sink.path}/{this.sink.schemaSinkPathSegment}";
        }

        private string GetMetadataPath()
        {
            return $"{this.sink.path}/{this.sink.metadataSinkPathSegment}";
        }

        private Task<Option<UploadedBlob>> CreateSchemaFile()
        {
            var (fullHash, shortHash, schemaBytes) = this.sink.parquetSchema.GetSchemaHash();
            this.schemaHash = shortHash;
            this.Log.Info("Schema hash length for this source: {schemaByteLength}", schemaBytes.Length);
            this.Log.Info("Full schema hash for this source: {schemaHash}", fullHash);

            var schemaId = Guid.NewGuid();
            // Save empty file to base output location and schema store
            return this.sink.storageWriter.SaveBytesAsBlob(new BinaryData(schemaBytes), this.GetSavePath(),
                    $"part-{schemaId}-{this.schemaHash}-chunk.parquet")
                .Map(_ => this.sink.storageWriter.SaveBytesAsBlob(new BinaryData(schemaBytes), this.GetSchemaPath(),
                    $"schema-{schemaId}-{this.schemaHash}.parquet"))
                .Flatten()
                .TryMap(s => s.AsOption(), this.HandleError);
        }

        private Task<Option<UploadedBlob>> SavePart()
        {
            return this.sink.storageWriter.SaveBytesAsBlob(new BinaryData(this.memoryStream.ToArray()),
                this.GetSavePath(),
                string.IsNullOrEmpty(this.schemaHash)
                    ? $"part-{Guid.NewGuid()}-chunk.parquet"
                    : $"part-{Guid.NewGuid()}-{this.schemaHash}-chunk.parquet")
                .TryMap(s => s.AsOption(), this.HandleError);
        }

        private Task<Option<UploadedBlob>> SaveCompletionToken()
        {
            if (this.interruptionToken.IsInterrupted)
            {
                this.Log.Info("Stream was interrupted, not saving completion token");
                return Task.FromResult(Option<UploadedBlob>.None);
            }
            if (this.sink.dropCompletionToken)
                // there seems to be an issue with Moq library and how it serializes BinaryData type
                // in order to have consistent behaviour between units and actual runs we write byte 0 to the file
            {
                return this.sink.storageWriter.SaveBytesAsBlob(new BinaryData(new byte[] { 0 }), this.GetSavePath(),
                    $"{this.schemaHash}.COMPLETED")
                .TryMap(s => s.AsOption(), this.HandleError);
            }

            return Task.FromResult(Option<UploadedBlob>.None);
        }

        private void WriteRowGroup(List<ParquetColumn> parquetColumns)
        {
            if (parquetColumns.Count == 0)
            {
                this.Log.Info("Received and empty chunk for {sinkPath}", this.sink.path);
                return;
            }

            if (this.blockCount == 0)
            {
                this.memoryStream = new MemoryStream();
            }

            this.writeInProgress = true;
            this.blockCount += 1;
            this.Log.Info("Processing inlet {blockCount} for {sinkPath}, size {dataLength}", this.blockCount,
                this.sink.path,
                parquetColumns[0].Data.Length);
            try
            {
                using (var parquetWriter = new ParquetWriter(this.sink.parquetSchema, this.memoryStream,
                           append: this.sink.rowGroupsPerFile > 1 && this.blockCount > 1))
                {
                    using (var groupWriter = parquetWriter.CreateRowGroup())
                    {
                        foreach (var parquetColumn in parquetColumns)
                        {
                            groupWriter.WriteColumn(parquetColumn);
                        }
                    }
                }

                if (this.blockCount % this.sink.rowGroupsPerFile == 0 || this.IsClosed(this.sink.In))
                {
                    this.SavePart().ContinueWith(_ => this.GetAsyncCallback(this.PullOrComplete).Invoke());
                    this.blockCount = 0;
                }
                else
                {
                    this.PullOrComplete();
                }
            }
            catch (Exception ex)
            {
                switch (this.decider.Decide(ex))
                {
                    case Directive.Stop:
                        this.taskCompletion.TrySetException(ex);
                        this.FailStage(ex);
                        break;
                    case Directive.Resume:
                        this.WriteRowGroup(parquetColumns);
                        break;
                    case Directive.Restart:
                        this.PullOrComplete();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        private void CompleteSink()
        {
            this.taskCompletion.TrySetResult(NotUsed.Instance);
            this.CompleteStage();
        }

        private void Finish()
        {
            if (this.memoryStream is { Length: > 0 })
            {
                this.SavePart().Map(_ => this.SaveCompletionToken()).Flatten()
                    .ContinueWith(_ => this.GetAsyncCallback(this.CompleteSink).Invoke());
            }
            else
            {
                this.SaveCompletionToken().ContinueWith(_ => this.GetAsyncCallback(this.CompleteSink).Invoke());
            }
        }

        private void PullOrComplete()
        {
            this.writeInProgress = false;
            if (this.IsClosed(this.sink.In))
            {
                this.Finish();
            }
            else
            {
                this.Pull(this.sink.In);
            }
        }

        private Option<UploadedBlob> HandleError(Exception ex)
        {
            var directive = this.decider.Decide(ex);
            switch (directive)
            {
                case Directive.Stop:
                case Directive.Restart:
                case Directive.Escalate:
                    this.taskCompletion.TrySetException(ex);
                    this.FailStage(ex);
                    return Option<UploadedBlob>.None;
                case Directive.Resume:
                    return Option<UploadedBlob>.None;
            }
            return Option<UploadedBlob>.None;
        }
    }
}
