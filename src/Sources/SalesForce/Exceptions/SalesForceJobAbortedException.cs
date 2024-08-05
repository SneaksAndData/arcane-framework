using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Sources.SalesForce.Exceptions;

/// <summary>
/// Thrown if the Salesforce job was aborted.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public class SalesForceJobAbortedException : SalesForceJobException
{
    public SalesForceJobAbortedException(string message) : base(message)
    {
    }

    public SalesForceJobAbortedException(string message, Exception inner) : base(message, inner)
    {
    }
}
