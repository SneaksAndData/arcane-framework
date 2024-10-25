using Arcane.Framework.Services.Models;
using Arcane.Framework.Sinks.Models;

namespace Arcane.Framework.Sinks.Extensions;

/// <summary>
/// Extension methods for StreamPartition class
/// </summary>
public static class StreamPartitionExtensions
{
    /// <summary>
    /// Converts a PartitionsMetadataDefinition to a StreamPartition
    /// </summary>
    /// <param name="partition"></param>
    /// <returns></returns>
    public static StreamPartition ToStreamPartition(this PartitionMetadataDefinition partition)
    {
        return new StreamPartition
        {
            Description = partition.Description,
            FieldName = partition.FieldName,
            FieldFormat = partition.FieldFormat,
            FieldExpression = (partition as DatePartitionMetadataDefinition)?.FieldExpression,
            IsDatePartition = (partition as DatePartitionMetadataDefinition) is not null
        };
    }
}
