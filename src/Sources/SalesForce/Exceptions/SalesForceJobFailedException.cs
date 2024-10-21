using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Sources.SalesForce.Exceptions;

/// <summary>
/// Thrown if the Salesforce job return with failed status.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public class SalesForceJobFailedException : SalesForceJobException
{
    public SalesForceJobFailedException(string message) : base(message)
    {
    }

    public SalesForceJobFailedException(string message, Exception inner) : base(message, inner)
    {
    }
}
