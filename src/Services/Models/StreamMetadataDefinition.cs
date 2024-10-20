using System.Text.Json.Serialization;

namespace Arcane.Framework.Services.Models;

/// <summary>
/// Used to deserialize the stream metadata from the Kubernetes object definition.
/// This class should be used by the StreamContext implementation in the Stream plugin.
/// </summary>
public class StreamMetadataDefinition
{
    /// <summary>
    /// Partitioning information about the stream, datetime-based
    /// Can be either field or an expression to be executed on the engine.
    /// </summary>
    [JsonPropertyName("partitions")]
    public DatePartitionMetadataDefinition DatePartition { get; init; }

    /// <summary>
    /// Partitioning information about the stream (non-datetime based)
    /// Only fields that are present in the data can be used here
    /// </summary>
    [JsonPropertyName("partitions")]
    public PartitionMetadataDefinition[] Partitions { get; init; }
}
