namespace Arcane.Framework.Services.Base;

/// <summary>
/// Provides red-only access to the stream configuration properties and the stream metadata to the stream stages.
/// </summary>
public interface IStreamContext
{
    /// <summary>
    /// Id of the stream
    /// </summary>
    string StreamId { get; }

    /// <summary>
    /// True if stream is running in backfill (full reload) mode
    /// </summary>
    bool IsRunningInBackfillMode { get; }

    /// <summary>
    /// Kind of the custom resource that manages the stream
    /// </summary>
    string StreamKind { get; }
}
