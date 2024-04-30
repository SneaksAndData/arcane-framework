using System.Diagnostics.CodeAnalysis;
using Snd.Sdk.Hosting;

namespace Arcane.Framework.Providers.Hosting;

/// <summary>
/// Class that provides context for the stream host builder and data for the stream context
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Model")]
public class StreamingHostBuilderContext
{
    private StreamingHostBuilderContext(string prefix)
    {
        var environmentPrefix = prefix ?? EnvironmentExtensions.GetAssemblyVariablePrefix();
        this.StreamId = EnvironmentExtensions.GetAssemblyEnvironmentVariable($"{environmentPrefix}STREAM_ID");

        this.IsBackfilling = EnvironmentExtensions.GetAssemblyEnvironmentVariable($"{environmentPrefix}BACKFILL")
            .Equals("true", System.StringComparison.InvariantCultureIgnoreCase);

        this.StreamKind = EnvironmentExtensions.GetAssemblyEnvironmentVariable($"{environmentPrefix}STREAM_KIND");
    }


    /// <summary>
    /// Id of the stream
    /// </summary>
    public string StreamId { get; }

    /// <summary>
    /// True if stream is running in backfill (full reload) mode
    /// </summary>
    public bool IsBackfilling { get;  }

    /// <summary>
    /// Kind of the custom resource that manages the stream
    /// </summary>
    public string StreamKind { get; }

    /// <summary>
    /// Application name for the stream for observability services
    /// </summary>
    public string ApplicationName => $"Arcane.Stream.{this.StreamKind}";

    /// <summary>
    /// Creates a new instance of the StreamingHostBuilderContext with values from the environment variables
    /// </summary>
    public static StreamingHostBuilderContext FromEnvironment(string prefix) => new(prefix);
}
