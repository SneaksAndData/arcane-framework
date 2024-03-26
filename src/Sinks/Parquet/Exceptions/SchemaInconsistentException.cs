using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Sinks.Parquet.Exceptions;

/// <summary>
/// Thrown if the schema of the source is inconsistent with the schema of the sink
/// </summary>
[ExcludeFromCodeCoverage]
public class SchemaInconsistentException : Exception
{
    /// <summary>
    /// Thrown if the schema of the source is inconsistent with the schema of the sink
    /// </summary>
    /// <param name="sourceFields">Number of source fields</param>
    /// <param name="sinkFields">Number of sink fields</param>
    public SchemaInconsistentException(int sourceFields, int sinkFields)
    {
        Message = "Source schema is inconsistent with the schema Sink was instantiated with. " +
                  $"Source has {sourceFields} fields, sink has {sinkFields} fields";
    }

    /// <summary>
    /// Exception message
    /// </summary>
    public override string Message { get; }
}
