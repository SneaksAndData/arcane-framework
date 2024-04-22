namespace Arcane.Framework.Services.Base;

/// <summary>
/// Read-write stream context options used by the application configuration pipeline
/// This interface should be implemented by the streaming plugin.
/// </summary>
public interface IStreamOptions: IStreamContext
{
    /// <summary>
    /// Id of the stream
    /// </summary>
    public string StreamId { get; set; }

    /// <summary>
    /// True if stream is running in backfill (full reload) mode
    /// </summary>
    public bool IsRunningInBackfillMode { get; set; }

    /// <summary>
    /// Kind of the custom resource that manages the stream
    /// </summary>
    public string StreamKind{ get; set;  }
}
