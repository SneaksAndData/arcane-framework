using Akka.Util;

namespace Arcane.Framework.Sinks.Models;

public record StreamPartition(string FieldName, string FieldFormat);

public record StreamMetadata(Option<StreamPartition[]> Partitions);
