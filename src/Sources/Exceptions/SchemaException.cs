using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Sources.Exceptions;

/// <summary>
/// Base class for all schema-related exceptions
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public class SchemaException : Exception
{
}
