using System.Text.Json.Serialization;

namespace Arcane.Framework.Services.Models;

/// <summary>
/// Used to deserialize the stream metadata from the Kubernetes object definition.
/// </summary>
public class PartitionsMetadataDefinition
{
    /// <summary>
    /// Partition name
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; init; }

    /// <summary>
    /// Partition field name
    /// </summary>
    [JsonPropertyName("fieldName")]
    public string FieldName { get; init; }

    /// <summary>
    /// Partition field format
    /// </summary>
    [JsonPropertyName("fieldFormat")]
    public string FieldFormat { get; init; }
}
