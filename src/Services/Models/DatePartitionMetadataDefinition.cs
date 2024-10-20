using System.Text.Json.Serialization;

namespace Arcane.Framework.Services.Models;

/// <summary>
/// Used to deserialize the stream metadata from the Kubernetes object definition.
/// </summary>
public class DatePartitionMetadataDefinition : PartitionMetadataDefinition
{
    /// <inheritdoc/>
    public override string FieldName => "";

    /// <inheritdoc/>
    public override string FieldFormat => "";

    /// <summary>
    /// Expression to compute the partition value for each row - overrides FieldName and FieldFormat
    /// Expression will be executed on Source side and thus will depend on backend engine the Source streams from.
    /// Common use case for this is SQL-based sources, to generate a partition derived from several columns.
    /// </summary>
    [JsonPropertyName("fieldExpression")]
    public string FieldExpression { get; init; }
}
