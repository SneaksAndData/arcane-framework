using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Arcane.Framework.Services.Base;
using Snd.Sdk.Helpers;

namespace Arcane.Framework.Contracts;

/// <summary>
/// Source tags for a stream metrics.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public sealed class SourceTags
{
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
    public SortedDictionary<string, string> GetAsDictionary(IStreamContext streamContext, string streamId) =>
        new()
        {
            { "arcane.sneaksanddata.com/kind", CodeExtensions.CamelCaseToSnakeCase(streamContext.StreamKind) },
            { "arcane.sneaksanddata.com/mode", streamContext.IsBackfilling ? "backfill" : "stream" },
            { "arcane.sneaksanddata.com/stream_source_location", this.SourceLocation },
            { "arcane.sneaksanddata.com/stream_source_entity", this.SourceEntity },
            { "arcane.sneaksanddata.com/stream_id", streamId }
        };
}
