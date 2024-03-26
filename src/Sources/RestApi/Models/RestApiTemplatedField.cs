using System.Text.Json.Serialization;

namespace Arcane.Framework.Sources.RestApi.Models;

/// <summary>
/// Represents a templated field in a REST API request or request body.
/// </summary>
public record RestApiTemplatedField
{
    /// <summary>
    /// Gets or sets the type of the templated field.
    /// </summary>
    [JsonPropertyName("fieldType")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public TemplatedFieldType FieldType { get; init; }

    /// <summary>
    /// Gets or sets the name of the templated field.
    /// </summary>
    [JsonPropertyName("fieldName")]
    public string FieldName { get; init; }

    /// <summary>
    /// Gets or sets the format string of the templated field.
    /// </summary>
    [JsonPropertyName("formatString")]
    public string FormatString { get; init; }

    /// <summary>
    /// Gets or sets the placement of the templated field.
    /// </summary>
    [JsonPropertyName("placement")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public TemplatedFieldPlacement Placement { get; init; }
}
