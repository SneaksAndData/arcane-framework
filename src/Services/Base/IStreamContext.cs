namespace Arcane.Framework.Services.Base;

/// <summary>
/// Provides common streaming job configuration properties
/// </summary>
public interface IStreamContext
{
    /// <summary>
    /// Id of the stream
    /// </summary>
    public string StreamId { get; }

    /// <summary>
    /// True if stream is running in backfill (full reload) mode
    /// </summary>
    public bool IsRunningInBackfillMode { get; }
}
