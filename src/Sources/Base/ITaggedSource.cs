using Arcane.Framework.Contracts;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// A Source that exposes certain attributes as metric tags.
/// </summary>
public interface ITaggedSource
{
    /// <summary>
    /// Return standardised Arcane metric tags.
    /// </summary>
    /// <returns></returns>
    SourceTags GetDefaultTags();
}
