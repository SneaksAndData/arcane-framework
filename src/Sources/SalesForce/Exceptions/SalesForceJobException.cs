using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Sources.SalesForce.Exceptions;

/// <summary>
/// Base class for all Salesforce-related exceptions
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public class SalesForceJobException : Exception
{
    public SalesForceJobException(string message)
        : base(message)
    {
    }

    public SalesForceJobException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
