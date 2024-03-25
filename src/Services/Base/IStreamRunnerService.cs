using System;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace Arcane.Framework.Services.Base;

/// <summary>
/// Service that runs a single stream instance
/// </summary>
public interface IStreamRunnerService
{
    /// <summary>
    /// Start a new stream, and save it's KillSwitch
    /// </summary>
    /// <param name="streamFactory">Factory function that creates a runnable stream object</param>
    /// <returns>Task that completes when stream is finished</returns>
    public Task RunStream(Func<IRunnableGraph<(UniqueKillSwitch, Task)>> streamFactory);

    /// <summary>
    /// Stop a running stream
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if stream stop requested before stream was started</exception>
    public void StopStream();
}
