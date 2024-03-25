using System;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Services.Base;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Arcane.Framework.Services;

/// <inheritdoc/>
public class StreamRunnerService : IStreamRunnerService

{
    private readonly IMaterializer materializer;
    private readonly ILogger<StreamRunnerService> logger;
    private readonly IHostApplicationLifetime applicationLifetime;

    private UniqueKillSwitch killSwitch;
    private Task streamTask;
    private readonly string streamId;

    public StreamRunnerService(
        IMaterializer materializer,
        ILogger<StreamRunnerService> logger,
        IHostApplicationLifetime applicationLifetime,
        IStreamConfigurationProvider streamConfigurationProvider)
    {
        this.materializer = materializer;
        this.logger = logger;
        this.applicationLifetime = applicationLifetime;
        this.streamId = streamConfigurationProvider.StreamId;
    }

    /// <inheritdoc/>
    public Task RunStream(Func<IRunnableGraph<(UniqueKillSwitch, Task)>> streamFactory)
    {
        (this.killSwitch, this.streamTask) = streamFactory().Run(materializer);
        this.applicationLifetime.ApplicationStopped.Register(this.StopStream);
        this.logger.LogInformation("Started stream with id {streamId}", this.streamId);
        return this.streamTask;
    }

    /// <inheritdoc/>
    public void StopStream()
    {
        if (this.killSwitch == null)
        {
            throw new InvalidOperationException("Requested to stop a stream that didn't start correctly.");
        }
        this.logger.LogInformation("Requested shutdown of a stream with id {streamId}", this.streamId);
        this.killSwitch.Shutdown();
    }
}
