using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Sources.Exceptions;

/// <summary>
/// Thrown if the schema of the source changed
/// </summary>
[ExcludeFromCodeCoverage]
public class SchemaMismatchException : Exception
{
    /// <summary>
    /// Thrown if the schema of the source changed
    /// </summary>
    /// <param name="underlying">Underlying exception</param>
    public SchemaMismatchException(Exception underlying)
    {
        StackTrace = underlying.StackTrace;
        Message = string.Join(Environment.NewLine, "Data source schema has been updated", underlying.Message);
    }

    /// <summary>
    /// Thrown if the schema of the source changed
    /// </summary>
    public SchemaMismatchException()
    {
        Message = "Data source schema has been updated";
    }

    /// <inheritdoc cref="Exception.StackTrace"/>
    public override string StackTrace { get; }

    /// <inheritdoc cref="Exception.Message"/>
    public override string Message { get; }
}
