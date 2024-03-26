using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Sources.CdmChangeFeedSource.Exceptions;

/// <summary>
/// Thrown if the schema for the entity is not found
/// </summary>
[ExcludeFromCodeCoverage]
public class SchemaNotFoundException : Exception
{
    /// <summary>
    /// Creates a new instance of <see cref="SchemaNotFoundException"/>
    /// </summary>
    /// <param name="sourceEntityName">Source entity name</param>
    public SchemaNotFoundException(string sourceEntityName) :
        base($"Could not found schema for entity: {sourceEntityName}")
    {
    }
}
