using System.Text.Json.Serialization;

namespace Arcane.Framework.Services.Models;

/// <summary>
/// Used to deserialize the stream metadata from the Kubernetes object definition.
/// </summary>
public class PartitionMetadataDefinition
{
    /// <summary>
    /// Partition name
    /// </summary>
    [JsonPropertyName("description")]
    public string Description { get; init; }

    /// <summary>
    /// Partition field name
    /// </summary>
    [JsonPropertyName("fieldName")]
    public virtual string FieldName { get; init; }

    /// <summary>
    /// Partition field format
    /// </summary>
    [JsonPropertyName("fieldFormat")]
    public virtual string FieldFormat { get; init; }
}
