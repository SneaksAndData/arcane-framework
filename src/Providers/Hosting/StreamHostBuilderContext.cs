using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Providers.Hosting;

/// <summary>
/// Class that provides context for the stream host builder and data for the stream context
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamingHostBuilderContext
{
    /// <summary>
    /// Id of the stream
    /// </summary>
    public string StreamId { get; init; }

    /// <summary>
    /// True if stream is running in backfill (full reload) mode
    /// </summary>
    public bool IsBackfilling { get; init; }

    /// <summary>
    /// Kind of the custom resource that manages the stream
    /// </summary>
    public string StreamKind { get; init; }

    /// <summary>
    /// Application name for the stream for observability services
    /// </summary>
    public string ApplicationName => "Arcane.Stream";

    /// <summary>
    /// Creates a new instance of the StreamingHostBuilderContext with values from the environment variables
    /// </summary>
    public static StreamingHostBuilderContext FromEnvironment(string prefix)
    {
        var environmentPrefix = prefix ?? throw new ArgumentNullException(nameof(prefix));
        var isBackfilling = Environment.GetEnvironmentVariable($"{environmentPrefix}BACKFILL") ??
                            throw new ArgumentNullException($"{environmentPrefix}BACKFILL");
        return new StreamingHostBuilderContext
        {
            StreamId = Environment.GetEnvironmentVariable($"{environmentPrefix}STREAM_ID"),
            IsBackfilling = isBackfilling.Equals("true", StringComparison.InvariantCultureIgnoreCase),
            StreamKind = Environment.GetEnvironmentVariable($"{environmentPrefix}STREAM_KIND")
        };
    }
}
