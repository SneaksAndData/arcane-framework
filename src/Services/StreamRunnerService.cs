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
    private readonly IHostApplicationLifetime applicationLifetime;
    private readonly ILogger<StreamRunnerService> logger;
    private readonly IMaterializer materializer;
    private readonly string streamId;

    private UniqueKillSwitch killSwitch;
    private Task streamTask;

    /// <summary>
    /// Creates new instance of <see cref="StreamRunnerService"/>
    /// </summary>
    /// <param name="materializer">Akka stream materializer</param>
    /// <param name="logger">Logger instance</param>
    /// <param name="applicationLifetime">Application lifetime service</param>
    /// <param name="streamConfigurationProvider">Stream configuration provider service</param>
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
        (this.killSwitch, this.streamTask) = streamFactory().Run(this.materializer);
        this.applicationLifetime.ApplicationStopped.Register(StopStream);
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
