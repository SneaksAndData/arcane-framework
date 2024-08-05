using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Arcane.Framework.Sources.Base;
using Snd.Sdk.Storage.Base;

namespace Arcane.Framework.Sources.BlobStorage;

/// <summary>
/// Akka source that emits list of blobs in a blob container with a given prefix.
/// The source enumerates a cloud blob storage content (e.g. S3 or Azure Blob Container) and emits list of objects
/// that exist in the storage.
/// </summary>
public class BlobStorageSource : GraphStage<SourceShape<string>>
{
    private readonly string path;
    private readonly IBlobStorageListService blobStorageService;
    private readonly TimeSpan changeCaptureInterval;

    private BlobStorageSource(string path, IBlobStorageListService blobStorageService,
        TimeSpan changeCaptureInterval)
    {
        this.path = path;
        this.blobStorageService = blobStorageService;
        this.changeCaptureInterval = changeCaptureInterval;
        this.Shape = new SourceShape<string>(this.Out);
    }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(BlobStorageSource));

    /// <summary>
    /// Source outlet
    /// </summary>
    public Outlet<string> Out { get; } = new($"{nameof(BlobStorageSource)}.Out");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SourceShape<string> Shape { get; }

    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new SourceLogic(this);
    }

    /// <summary>
    /// Creates a <see cref="Source"/> for a cloud blob storage container.
    /// </summary>
    /// <param name="path">Filter objects by prefix</param>
    /// <param name="blobStorageService">Blob storage service instance</param>
    /// <param name="changeCaptureInterval">How often check for storage updates</param>
    /// <returns>BlobStorageSource instance</returns>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static BlobStorageSource Create(
        string path,
        IBlobStorageListService blobStorageService,
        TimeSpan changeCaptureInterval)
    {
        return new BlobStorageSource(path, blobStorageService, changeCaptureInterval);
    }

    private class SourceLogic : PollingSourceLogic
    {
        private const string TimerKey = nameof(SourceLogic);

        private readonly string path;
        private readonly IBlobStorageListService blobStorageService;
        private readonly LocalOnlyDecider decider;
        private readonly BlobStorageSource source;

        private IEnumerable<string> blobs;

        public SourceLogic(BlobStorageSource source) : base(source.changeCaptureInterval, source.Shape)
        {
            this.source = source;
            this.path = source.path;
            this.blobStorageService = source.blobStorageService;
            this.blobs = Enumerable.Empty<string>();
            this.decider = Decider.From((ex) => ex.GetType().Name switch
            {
                nameof(TimeoutException) => Directive.Restart,
                _ => Directive.Stop
            });

            this.SetHandler(source.Out, this.OnPull);
        }

        public override void PreStart()
        {
            this.GetBlobs();
        }

        protected override void OnTimer(object timerKey)
        {
            try
            {
                this.GetBlobs();
                this.OnPull();
            }
            catch (Exception ex)
            {
                this.DecideOnFailure(ex);
            }
        }

        private void DecideOnFailure(Exception ex)
        {
            switch (this.decider.Decide(ex))
            {
                case Directive.Stop:
                    this.FailStage(ex);
                    break;
                default:
                    this.ScheduleOnce(TimerKey, this.ChangeCaptureInterval);
                    break;
            }
        }

        private void OnPull()
        {
            if (this.blobs.Any())
            {
                this.EmitMultiple(this.source.Out, this.blobs);
                this.blobs = Enumerable.Empty<string>();
            }
            this.ScheduleOnce(TimerKey, this.ChangeCaptureInterval);
        }

        private void GetBlobs()
        {
            this.blobs = this.blobStorageService.ListBlobsAsEnumerable(this.path).Select(s => s.Name).ToList();
        }
    }
}
