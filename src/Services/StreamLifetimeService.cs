using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Arcane.Framework.Services.Base;
using Microsoft.Extensions.Logging;

namespace Arcane.Framework.Services;

/// <summary>
/// The default implementation of the <see cref="IStreamLifetimeService"/> interface.
/// Creates a service that terminates the stream in response to the SIGTERM signal.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Implementation is platform-specific")]
public class StreamLifetimeService : IStreamLifetimeService
{
    private readonly ILogger<StreamLifetimeService> logger;
    private readonly List<PosixSignalRegistration> registrations = new();
    private readonly IStreamRunnerService streamRunnerService;

    /// <summary>
    /// Create a new instance of the <see cref="StreamLifetimeService"/> class and register the signal handler.
    /// </summary>
    /// <param name="logger">Logger</param>
    /// <param name="streamRunnerService">A Stream runner owning a stream to be stopped</param>
    /// <param name="signal">The signal to be handled, defaults to SIGTERM</param>
    public StreamLifetimeService(ILogger<StreamLifetimeService> logger,
        IStreamRunnerService streamRunnerService, PosixSignal signal = PosixSignal.SIGTERM)
    {
        this.streamRunnerService = streamRunnerService;
        this.AddStreamTerminationSignal(signal);
        this.logger = logger;
    }

    /// <inheritdoc cref="IStreamLifetimeService.AddStreamTerminationSignal"/>>
    public void AddStreamTerminationSignal(PosixSignal posixSignal)
    {
        this.registrations.Add(PosixSignalRegistration.Create(posixSignal, this.StopStream));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        this.Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void StopStream(PosixSignalContext context)
    {
        context.Cancel = true;
        this.logger.LogInformation(
            "Received a signal {signal}. Stopping the hosted stream and shutting down application", context.Signal);
        this.streamRunnerService.StopStream();
    }

    /// <summary>
    /// Allows override object disposal in subclasses.
    /// </summary>
    /// <param name="disposing"></param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        foreach (var registration in this.registrations)
        {
            registration.Dispose();
        }
    }
}
