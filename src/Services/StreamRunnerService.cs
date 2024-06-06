using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Services.Base;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Arcane.Framework.Services;

/// <inheritdoc/>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
internal class StreamRunnerService : IStreamRunnerService

{
    private readonly IHostApplicationLifetime applicationLifetime;
    private readonly ILogger<StreamRunnerService> logger;
    private readonly IMaterializer materializer;

    private UniqueKillSwitch killSwitch;
    private Task streamTask;

    /// <summary>
    /// Creates new instance of <see cref="StreamRunnerService"/>
    /// </summary>
    /// <param name="materializer">Akka stream materializer</param>
    /// <param name="logger">Logger instance</param>
    /// <param name="applicationLifetime">Application lifetime service</param>
    public StreamRunnerService(
        IMaterializer materializer,
        ILogger<StreamRunnerService> logger,
        IHostApplicationLifetime applicationLifetime)
    {
        this.materializer = materializer;
        this.logger = logger;
        this.applicationLifetime = applicationLifetime;
    }

    /// <inheritdoc/>
    public Task RunStream(Func<IRunnableGraph<(UniqueKillSwitch, Task)>> streamFactory)
    {
        (this.killSwitch, this.streamTask) = streamFactory().Run(this.materializer);
        this.applicationLifetime.ApplicationStopped.Register(this.StopStream);
        this.logger.LogInformation("Stream started");
        return this.streamTask;
    }

    /// <inheritdoc/>
    public void StopStream()
    {
        if (this.killSwitch == null)
        {
            throw new InvalidOperationException("Requested to stop a stream that didn't start correctly.");
        }

        this.logger.LogInformation("Requested stream shutdown");
        this.killSwitch.Shutdown();
    }
}
