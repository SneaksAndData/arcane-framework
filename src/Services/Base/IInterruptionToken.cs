namespace Arcane.Framework.Services.Base;

/// <summary>
/// Provides information about a stream interruption
/// </summary>
public interface IInterruptionToken
{
    /// <summary>
    /// Returns true if the stream was interrupted
    /// </summary>
    public bool IsInterrupted { get; }
}
