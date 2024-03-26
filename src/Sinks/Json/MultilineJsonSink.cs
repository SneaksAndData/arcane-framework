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
using Arcane.Framework.Sinks.Parquet;
using Parquet.Data;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Sinks.Json;

/// <summary>
/// Sink that writes a list of JSON objects to a file divided by new lines.
/// </summary>
public class MultilineJsonSink : GraphStageWithMaterializedValue<SinkShape<List<JsonElement>>, Task>
{
    private readonly string dataPathSegment;
    private readonly bool dropCompletionToken;
    private readonly string jsonSinkPath;
    private readonly string schemaPathSegment;
    private readonly Schema sinkSchema;
    private readonly IBlobStorageWriter storageWriter;

    /// <summary>
    /// Creates a new instance of <see cref="JsonSink"/>
    /// </summary>
    private MultilineJsonSink(
        IBlobStorageWriter storageWriter,
        string jsonSinkPath,
        string dataPathSegment,
        string schemaPathSegment,
        Schema sinkSchema,
        bool dropCompletionToken)
    {
        this.storageWriter = storageWriter;
        this.jsonSinkPath = jsonSinkPath;
        this.dataPathSegment = dataPathSegment;
        this.dropCompletionToken = dropCompletionToken;
        this.sinkSchema = sinkSchema;
        this.schemaPathSegment = schemaPathSegment;

        Shape = new SinkShape<List<JsonElement>>(In);
    }

    /// <summary>
    /// Sink outlet
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
    /// <returns></returns>
    public static MultilineJsonSink Create(
        IBlobStorageWriter storageWriter,
        string jsonSinkPath,
        Schema sinkSchema,
        string dataPathSegment = "data",
        string schemaPathSegment = "schema",
        bool dropCompletionToken = false)
    {
        return new MultilineJsonSink(
            storageWriter,
            jsonSinkPath,
            sinkSchema: sinkSchema,
            dataPathSegment: dataPathSegment,
            schemaPathSegment: schemaPathSegment,
            dropCompletionToken: dropCompletionToken);
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

        public SinkLogic(MultilineJsonSink sink, TaskCompletionSource<NotUsed> taskCompletion) : base(sink.Shape)
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
                () => WriteJson(Grab(sink.In)),
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

            // dump empty schema file and then pull first element
            CreateSchemaFile().ContinueWith(_ => GetAsyncCallback(() => Pull(sink.In)).Invoke());
        }

        private Task<UploadedBlob> CreateSchemaFile()
        {
            var (fullHash, shortHash, schemaBytes) = sink.sinkSchema.GetSchemaHash();
            schemaHash = shortHash;
            Log.Info("Schema hash length for this source: {schemaByteLength}", schemaBytes.Length);
            Log.Info("Full schema hash for this source: {schemaHash}", fullHash);

            var schemaId = Guid.NewGuid();
            // Save empty file to base output location and schema store
            return sink.storageWriter.SaveBytesAsBlob(new BinaryData(schemaBytes),
                $"{sink.jsonSinkPath}/{sink.schemaPathSegment}",
                $"schema-{schemaId}-{schemaHash}.parquet");
        }

        private Task<UploadedBlob> SavePart()
        {
            if (memoryStream.ToArray().Length > 0)
                return sink.storageWriter.SaveBytesAsBlob(new BinaryData(memoryStream.ToArray()),
                    $"{sink.jsonSinkPath}/{sink.dataPathSegment}",
                    $"part-{Guid.NewGuid()}-{schemaHash}.json");

            return Task.FromResult(new UploadedBlob());
        }

        private Task<UploadedBlob> SaveCompletionToken()
        {
            if (sink.dropCompletionToken)
                // there seems to be an issue with Moq library and how it serializes BinaryData type
                // in order to have consistent behaviour between units and actual runs we write byte 0 to the file
                return sink.storageWriter.SaveBytesAsBlob(new BinaryData(new byte[] { 0 }),
                    $"{sink.jsonSinkPath}/{sink.dataPathSegment}",
                    $"{schemaHash}.COMPLETED");

            return Task.FromResult(new UploadedBlob());
        }

        private void WriteJson(List<JsonElement> batch)
        {
            writeInProgress = true;
            memoryStream = new MemoryStream();

            try
            {
                using (var writer = new StreamWriter(memoryStream))
                {
                    foreach (var element in batch) writer.WriteLine(JsonSerializer.Serialize(element));
                }

                SavePart().ContinueWith((_) => GetAsyncCallback(PullOrComplete).Invoke());
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
                        WriteJson(batch);
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
            if (memoryStream is { CanRead: true, Length: > 0 })
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
