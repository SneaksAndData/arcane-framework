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
        streamId = streamConfigurationProvider.StreamId;
    }

    /// <inheritdoc/>
    public Task RunStream(Func<IRunnableGraph<(UniqueKillSwitch, Task)>> streamFactory)
    {
        (killSwitch, streamTask) = streamFactory().Run(materializer);
        applicationLifetime.ApplicationStopped.Register(StopStream);
        logger.LogInformation("Started stream with id {streamId}", streamId);
        return streamTask;
    }

    /// <inheritdoc/>
    public void StopStream()
    {
        if (killSwitch == null)
            throw new InvalidOperationException("Requested to stop a stream that didn't start correctly.");
        logger.LogInformation("Requested shutdown of a stream with id {streamId}", streamId);
        killSwitch.Shutdown();
    }
}
