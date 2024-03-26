namespace Arcane.Framework.Sources.RestApi.Models;

/// <summary>
/// Supported page listing algorithms.
/// </summary>
public enum PageResolverType
{
    /// <summary>
    /// Page resolver based on page counter.
    /// </summary>
    COUNTER,

    /// <summary>
    /// Page offset resolver based on offset.
    /// </summary>
    OFFSET,

    /// <summary>
    /// Page resolver based on continuation token.
    /// </summary>
    TOKEN
}
