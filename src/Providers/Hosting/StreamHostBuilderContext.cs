using System.Diagnostics.CodeAnalysis;
using Snd.Sdk.Hosting;

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
    public string StreamId { get; } = EnvironmentExtensions.GetAssemblyEnvironmentVariable("STREAM_ID");

    /// <summary>
    /// True if stream is running in backfill (full reload) mode
    /// </summary>
    public bool IsBackfilling { get; } = EnvironmentExtensions
        .GetAssemblyEnvironmentVariable("BACKFILL")
        .Equals("true", System.StringComparison.InvariantCultureIgnoreCase);

    /// <summary>
    /// Kind of the custom resource that manages the stream
    /// </summary>
    public string StreamKind { get; } = (EnvironmentExtensions.GetAssemblyEnvironmentVariable("STREAM_KIND"));

    /// <summary>
    /// Application name for the stream for observability services
    /// </summary>
    public string ApplicationName => $"Arcane.Stream.{this.StreamKind}";

    /// <summary>
    /// Creates a new instance of the StreamingHostBuilderContext with values from the environment variables
    /// </summary>
    public static StreamingHostBuilderContext FromEnvironment() => new();
}
