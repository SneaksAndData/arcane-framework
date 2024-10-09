using System.Text.Json.Serialization;
using Akka.Util;

namespace Arcane.Framework.Sinks.Models;

/// <summary>
/// Stream partitioning information for consumers
/// </summary>
public class StreamPartition
{
    /// <summary>
    /// Partition name
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; init; }

    /// <summary>
    /// Partition field name
    /// </summary>
    [JsonPropertyName("field_name")]
    public string FieldName { get; init; }

    /// <summary>
    /// Partition field format
    /// </summary>
    [JsonPropertyName("field_format")]
    public string FieldFormat { get; init; }
}

/// <summary>
/// Represents stream metadata that can be used by the stream consumer
/// </summary>
/// <param name="Partitions">Partitioning information about the stream</param>
public record StreamMetadata(Option<StreamPartition[]> Partitions);
