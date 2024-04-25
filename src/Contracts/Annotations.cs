namespace Arcane.Framework.Contracts;

/// <summary>
/// Streaming job annotation keys that can be used by a StreamRunner and a Stream Operator
/// This class should be removed in future and replaced with Stream-class based contracts, see #23
/// </summary>
public class Annotations
{
    /// <summary>
    /// Annotation that signals to StreamOperator to start backfill process
    /// If the streaming job was marked with this annotation, StreamOperator stops the current stream and starts
    /// a backfill job.
    /// </summary>
    public const string STATE_ANNOTATION_KEY = "arcane.streaming.sneaksanddata.com/state";

    /// <summary>
    /// Annotation value signals to StreamOperator to start backfill process
    /// If the streaming job was marked with this annotation, StreamOperator stops the current stream and starts
    /// a backfill job.
    /// </summary>
    public const string SCHEMA_MISMATCH_STATE_ANNOTATION_VALUE = "schema-mismatch";
}
