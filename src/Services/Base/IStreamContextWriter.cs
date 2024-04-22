namespace Arcane.Framework.Services.Base;

/// <summary>
/// Read-write stream context options used by the application configuration pipeline
/// This interface should be implemented by the streaming plugin.
/// </summary>
public interface IStreamContextWriter
{
    /// <summary>
    /// Id of the stream
    /// </summary>
    void SetStreamId(string streamId);

    /// <summary>
    /// True if stream is running in backfill (full reload) mode
    /// </summary>
    void SetIsRunningInBackfillMode(bool isRunningInBackfillMode);

    /// <summary>
    /// Kind of the custom resource that manages the stream
    /// </summary>
    void SetStreamKind(string streamKind);
}
