using System.Collections.Generic;

namespace Arcane.Framework.Contracts;

/// <summary>
/// Source tags for a stream metrics.
/// </summary>
public sealed class SourceTags
{
    /// <summary>
    /// The Kubernetes object kind of the stream.
    /// </summary>
    public string StreamKind { get; set; }

    /// <summary>
    /// Source location in human readable format.
    /// </summary>
    public string SourceLocation { get; set; }

    /// <summary>
    /// Source entity name
    /// </summary>
    public string SourceEntity { get; set; }

    /// <summary>
    /// Converts to dictionary.
    /// </summary>
    /// <returns></returns>
    public SortedDictionary<string, string> GetAsDictionary()
    {
        return new SortedDictionary<string, string>
        {
            { "stream_source", this.StreamKind },
            { "stream_source_location", this.SourceLocation },
            { "stream_source_entity", this.SourceEntity }
        };
    }
}
