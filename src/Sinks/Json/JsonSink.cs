﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Stage;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;

namespace Arcane.Framework.Sinks.Json;

/// <summary>
/// Sink that writes incoming elements as JSON files to a BLOB storage
/// </summary>
public class JsonSink : GraphStageWithMaterializedValue<SinkShape<(string, List<(DateTimeOffset, JsonDocument)>)>, Task>
{
    private readonly string jsonFileName;
    private readonly string jsonSinkPath;
    private readonly IBlobStorageWriter storageWriter;

    /// <summary>
    /// Creates a new instance of <see cref="JsonSink"/>
    /// </summary>
    protected JsonSink(IBlobStorageWriter storageWriter, string jsonSinkPath, string jsonFileName)
    {
        this.storageWriter = storageWriter;
        this.jsonSinkPath = jsonSinkPath;
        this.jsonFileName = jsonFileName;

        this.Shape = new SinkShape<(string, List<(DateTimeOffset, JsonDocument)>)>(this.In);
    }

    /// <summary>
    /// Sink inlet
    /// </summary>
    public Inlet<(string, List<(DateTimeOffset, JsonDocument)>)> In { get; } = new($"{nameof(JsonSink)}.In");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SinkShape<(string, List<(DateTimeOffset, JsonDocument)>)> Shape { get; }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(JsonSink));

    /// <summary>
    /// Creates a new instance of <see cref="JsonSink"/>
    /// </summary>
    /// <param name="storageWriter">Blob storage service</param>
    /// <param name="jsonSinkPath">Sink path</param>
    /// <param name="jsonFileName">JSON file name ending</param>
    /// <returns></returns>
    public static JsonSink Create(IBlobStorageWriter storageWriter, string jsonSinkPath, string jsonFileName = "chunk")
    {
        return new JsonSink(storageWriter, jsonSinkPath, jsonFileName);
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
        private readonly JsonSink sink;
        private readonly TaskCompletionSource<NotUsed> taskCompletion;
        private string currentSavePath;
        private MemoryStream memoryStream;
        private bool writeInProgress;

        public SinkLogic(JsonSink sink, TaskCompletionSource<NotUsed> taskCompletion) :
            base(sink.Shape)
        {
            this.sink = sink;
            this.taskCompletion = taskCompletion;
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
            // Request the first element
            this.Pull(this.sink.In);
        }

        private Task<UploadedBlob> SavePart()
        {
            return this.sink.storageWriter.SaveBytesAsBlob(new BinaryData(this.memoryStream.ToArray()),
                this.currentSavePath,
                $"part-{Guid.NewGuid()}-{this.sink.jsonFileName}");
        }

        private void WriteJson((string, List<(DateTimeOffset, JsonDocument)>) batch)
        {
            var (path, data) = batch;
            this.currentSavePath = $"{this.sink.jsonSinkPath}{path}";
            this.writeInProgress = true;
            this.memoryStream = new MemoryStream();

            try
            {
                using (var writer = new StreamWriter(this.memoryStream))
                {
                    foreach (var doc in data)
                    {
                        var newDoc = new
                        {
                            timestamp = doc.Item1,
                            body = doc.Item2
                        };

                        writer.WriteLine(JsonSerializer.Serialize(newDoc));
                    }
                }

                this.SavePart().ContinueWith(_ => this.GetAsyncCallback(this.PullOrComplete).Invoke());
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
            if (this.memoryStream != null && this.memoryStream.CanRead && this.memoryStream.Length > 0)
            {
                this.SavePart().ContinueWith(_ => this.GetAsyncCallback(this.CompleteSink).Invoke());
            }
            else
            {
                this.CompleteSink();
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
