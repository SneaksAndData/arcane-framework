using System.Text.Json.Serialization;

namespace Arcane.Framework.Services.Models;

/// <summary>
/// Used to deserialize the stream metadata from the Kubernetes object definition.
/// This class should be used by the StreamContext implementation in the Stream plugin.
/// </summary>
public class StreamMetadataDefinition
{
    /// <summary>
    /// Partitioning information about the stream.
    /// </summary>
    [JsonPropertyName("partitions")]
    public PartitionsMetadataDefinition[] Partitions { get; init; }
}
