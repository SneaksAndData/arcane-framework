namespace Arcane.Framework.Sources.Base;

/// <summary>
/// An interface for a graph that can stop after a backfill
/// </summary>
public interface IStopAfterBackfill
{
    /// <summary>
    /// Returns True if source logic should complete the stage after a full load is finished
    /// </summary>
    bool StopAfterBackfill { get; }

    /// <summary>
    /// True if source logic works in backfill mode.
    /// When full load is finished and source started stream updates, this should be set ta false
    /// </summary>
    bool IsRunningInBackfillMode { get; set; }
}
