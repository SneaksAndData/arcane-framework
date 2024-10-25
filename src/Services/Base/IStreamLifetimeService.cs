using System;
using System.Runtime.InteropServices;

namespace Arcane.Framework.Services.Base;

/// <summary>
/// Service to manage the lifetime of a stream runner
/// </summary>
public interface IStreamLifetimeService: IDisposable, IInterruptionToken
{
    /// <summary>
    /// Add a signal to listen for to stop the stream
    /// </summary>
    /// <param name="posixSignal">POSIX signal</param>
    /// <returns></returns>
    void AddStreamTerminationSignal(PosixSignal posixSignal);

    /// <summary>
    /// Returns true if a stop request has been made
    /// </summary>
    bool IsStopRequested { get; }
}
