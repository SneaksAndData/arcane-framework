using Akka.Util;
using Arcane.Framework.Sinks.Models;

namespace Arcane.Framework.Services.Base;

/// <summary>
/// Provides red-only access to the stream configuration properties and the stream metadata to the stream stages.
/// </summary>
public interface IStreamContext
{
    /// <summary>
    /// The stream identifier.
    /// </summary>
    string StreamId { get; }

    /// <summary>
    /// True if stream is running in backfill (full reload) mode.
    /// </summary>
    bool IsBackfilling { get; }

    /// <summary>
    /// Kind of the custom resource that manages the stream.
    /// </summary>
    string StreamKind { get; }

    /// <summary>
    /// Stream metadata that can be used by the stream consumer.
    /// </summary>
    Option<StreamMetadata> StreamMetadata { get; }
}
