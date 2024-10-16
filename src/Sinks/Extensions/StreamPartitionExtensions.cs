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
    public static StreamPartition ToStreamPartition(this PartitionsMetadataDefinition partition)
    {
        return new StreamPartition
        {
            Name = partition.Name,
            FieldName = partition.FieldName,
            FieldFormat = partition.FieldFormat
        };
    }
}
