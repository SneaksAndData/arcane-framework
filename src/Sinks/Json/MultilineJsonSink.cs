using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Stage;
using Arcane.Framework.Sinks.Extensions;
using Arcane.Framework.Sinks.Models;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Sinks.Services.Base;
using Parquet.Data;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Sinks.Json;

/// <summary>
/// Sink that writes incoming lists of elements into a file per list, each element as a JSON-serialised
/// line in the file, on a BLOB storage. Also optionally drops a schema for the output in a form of an empty PARQUET file.
/// </summary>
public class MultilineJsonSink : GraphStageWithMaterializedValue<SinkShape<List<JsonElement>>, Task>
{
    private readonly string dataPathSegment;
    private readonly bool dropCompletionToken;
    private readonly string jsonSinkPath;
    private readonly string schemaPathSegment;
    private readonly Schema sinkSchema;
    private readonly IBlobStorageWriter storageWriter;
    private readonly StreamMetadata streamMetadata;
    private readonly string metadataSinkPathSegment;
    private readonly StreamMetadata metadata;

    /// <summary>
    /// Creates a new instance of <see cref="JsonSink"/>
    /// </summary>
    private MultilineJsonSink(
        IBlobStorageWriter storageWriter,
        string jsonSinkPath,
        string dataPathSegment,
        string schemaPathSegment,
        Schema sinkSchema,
        bool dropCompletionToken,
        StreamMetadata streamMetadata,
        string metadataSinkPathSegment)
    {
        this.storageWriter = storageWriter;
        this.jsonSinkPath = jsonSinkPath;
        this.dataPathSegment = dataPathSegment;
        this.dropCompletionToken = dropCompletionToken;
        this.sinkSchema = sinkSchema;
        this.schemaPathSegment = schemaPathSegment;
        this.streamMetadata = streamMetadata;
        this.metadataSinkPathSegment = metadataSinkPathSegment;
        this.metadata = streamMetadata;

        this.Shape = new SinkShape<List<JsonElement>>(this.In);
    }

    /// <summary>
    /// Sink inlet
    /// </summary>
    public Inlet<List<JsonElement>> In { get; } = new($"{nameof(MultilineJsonSink)}.In");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SinkShape<List<JsonElement>> Shape { get; }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(MultilineJsonSink));

    /// <summary>
    /// Creates a new instance of <see cref="MultilineJsonSink"/>
    /// </summary>
    /// <param name="storageWriter">Blob storage writer service</param>
    /// <param name="jsonSinkPath">Path to drop data</param>
    /// <param name="sinkSchema"></param>
    /// <param name="dataPathSegment">Folder name to emit data</param>
    /// <param name="schemaPathSegment">Folder name to emit schema</param>
    /// <param name="dropCompletionToken">True if sink should drop a file when complete.</param>
    /// <param name="streamMetadata">Metadata that describes data produced by the stream</param>
    /// <param name="metadataSinkPathSegment">Folder name to emit metadata</param>
    /// <returns></returns>
    public static MultilineJsonSink Create(
        IBlobStorageWriter storageWriter,
        string jsonSinkPath,
        Schema sinkSchema,
        StreamMetadata streamMetadata,
        string dataPathSegment = "data",
        string schemaPathSegment = "schema",
        bool dropCompletionToken = false,
        string metadataSinkPathSegment = "metadata")
    {
        return new MultilineJsonSink(
            storageWriter,
            jsonSinkPath,
            sinkSchema: sinkSchema,
            dataPathSegment: dataPathSegment,
            schemaPathSegment: schemaPathSegment,
            dropCompletionToken: dropCompletionToken,
            streamMetadata: streamMetadata,
            metadataSinkPathSegment: metadataSinkPathSegment);
    }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.CreateLogicAndMaterializedValue"/>
    public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
    {
        var completion = new TaskCompletionSource<NotUsed>();
        return new LogicAndMaterializedValue<Task>(new SinkLogic(this, completion),
            completion.Task);
    }


    private class SinkLogic : GraphStageLogic
    {
        private readonly LocalOnlyDecider decider;
        private readonly MultilineJsonSink sink;
        private readonly TaskCompletionSource<NotUsed> taskCompletion;
        private MemoryStream memoryStream;
        private string schemaHash;
        private bool writeInProgress;
        private readonly IMetadataWriter metadataWriter;

        public SinkLogic(MultilineJsonSink sink, TaskCompletionSource<NotUsed> taskCompletion) : base(sink.Shape)
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

            this.SetHandler(sink.In,
                () => this.WriteJson(this.Grab(sink.In)),
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
            // dump empty schema file and then pull first element
            this.CreateSchemaFile().ContinueWith(_ => this.GetAsyncCallback(() => this.Pull(this.sink.In)).Invoke());
        }

        private Task<UploadedBlob> CreateSchemaFile()
        {
            var (fullHash, shortHash, schemaBytes) = this.sink.sinkSchema.GetSchemaHash();
            this.schemaHash = shortHash;
            this.Log.Info("Schema hash length for this source: {schemaByteLength}", schemaBytes.Length);
            this.Log.Info("Full schema hash for this source: {schemaHash}", fullHash);

            var schemaId = Guid.NewGuid();
            // Save empty file to base output location and schema store
            return this.sink.storageWriter.SaveBytesAsBlob(new BinaryData(schemaBytes),
                $"{this.sink.jsonSinkPath}/{this.sink.schemaPathSegment}",
                $"schema-{schemaId}-{this.schemaHash}.parquet");
        }

        private Task<UploadedBlob> SavePart()
        {
            if (this.memoryStream.ToArray().Length > 0)
            {
                return this.sink.storageWriter.SaveBytesAsBlob(new BinaryData(this.memoryStream.ToArray()),
                    $"{this.sink.jsonSinkPath}/{this.sink.dataPathSegment}",
                    $"part-{Guid.NewGuid()}-{this.schemaHash}.json");
            }

            return Task.FromResult(new UploadedBlob());
        }

        private string GetMetadataPath()
        {
            return $"{this.sink.jsonSinkPath}/{this.sink.metadataSinkPathSegment}";
        }

        private Task<UploadedBlob> SaveCompletionToken()
        {
            if (this.sink.dropCompletionToken)
                // there seems to be an issue with Moq library and how it serializes BinaryData type
                // in order to have consistent behaviour between units and actual runs we write byte 0 to the file
            {
                return this.sink.storageWriter.SaveBytesAsBlob(new BinaryData(new byte[] { 0 }),
                    $"{this.sink.jsonSinkPath}/{this.sink.dataPathSegment}",
                    $"{this.schemaHash}.COMPLETED");
            }

            return Task.FromResult(new UploadedBlob());
        }

        private void WriteJson(List<JsonElement> batch)
        {
            this.writeInProgress = true;
            this.memoryStream = new MemoryStream();

            try
            {
                using (var writer = new StreamWriter(this.memoryStream))
                {
                    foreach (var element in batch)
                    {
                        writer.WriteLine(JsonSerializer.Serialize(element));
                    }
                }

                this.SavePart().ContinueWith((_) => this.GetAsyncCallback(this.PullOrComplete).Invoke());
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
                        this.WriteJson(batch);
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
            if (this.memoryStream is { CanRead: true, Length: > 0 })
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
    }
}
