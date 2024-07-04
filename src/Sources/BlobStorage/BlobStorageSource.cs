using System;
using System.Collections.Generic;
using System.Linq;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Stage;
using Arcane.Framework.Contracts;
using Arcane.Framework.Sources.Base;
using Snd.Sdk.Storage.Base;

namespace Arcane.Framework.Sources.BlobStorage;

/// <summary>
/// Akka source that emits list of blobs in a blob container with a given prefix.
/// The source enumerates a cloud blob storage content (e.g. S3 or Azure Blob Container) ane emits list of objects
/// that exist in the storage.
/// </summary>
public class BlobStorageSource : GraphStage<SourceShape<string>>, ITaggedSource
{
    private readonly string prefix;
    private readonly IBlobStorageService blobStorageService;
    private readonly TimeSpan changeCaptureInterval;
    private readonly string blobContainer;

    private BlobStorageSource(string blobContainer, string prefix, IBlobStorageService blobStorageService,
        TimeSpan changeCaptureInterval)
    {
        this.prefix = prefix;
        this.blobStorageService = blobStorageService;
        this.changeCaptureInterval = changeCaptureInterval;
        this.blobContainer = blobContainer;
        this.Shape = new SourceShape<string>(this.Out);
    }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(CdmChangeFeedSource));

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

    /// <inheritdoc cref="ITaggedSource.GetDefaultTags"/>
    public SourceTags GetDefaultTags()
    {
        return new SourceTags
        {
            SourceEntity = this.blobContainer,
            SourceLocation = this.prefix
        };
    }

    private class SourceLogic : TimerGraphStageLogic
    {
        private const string TimerKey = nameof(SourceLogic);

        private readonly string prefix;
        private readonly IBlobStorageService blobStorageService;
        private readonly TimeSpan changeCaptureInterval;
        private readonly LocalOnlyDecider decider;
        private readonly BlobStorageSource source;

        private IEnumerable<string> blobs;

        public SourceLogic(BlobStorageSource source) : base(source.Shape)
        {
            this.source = source;
            this.prefix = source.prefix;
            this.blobStorageService = source.blobStorageService;
            this.changeCaptureInterval = source.changeCaptureInterval;
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
                    this.ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
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
            else
            {
                this.ScheduleOnce(TimerKey, this.changeCaptureInterval);
            }
        }

        private void GetBlobs()
        {
            this.blobs = this.blobStorageService.ListBlobsAsEnumerable(this.prefix).Select(s => s.Name).ToList();
        }
    }
}
