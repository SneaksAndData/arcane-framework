using System;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Services.Extensions;
using Arcane.Stream.RestApi.Models;
using Newtonsoft.Json;

namespace Arcane.Framework.Services;

/// <summary>
/// Provides access to a streaming job configuration
/// </summary>
public class StreamConfigurationReader: IStreamConfigurationReader
{
    private const string STREAM_KIND = nameof(STREAM_KIND);
    private const string STREAM_ID = nameof(STREAM_ID);
    private const string FULL_LOAD = nameof(FULL_LOAD);
    private const string SPEC = nameof(SPEC);

    public string StreamId => STREAM_ID.GetEnvironmentVariable();
    public string StreamKind => STREAM_KIND.GetEnvironmentVariable();

    public bool IsRunningInBackfillMode  => FULL_LOAD.GetEnvironmentVariable().Equals("true", StringComparison.OrdinalIgnoreCase);

    public TConfiguration Read<TConfiguration>(Action<TConfiguration> configureStreamConfiguration = null)
        where TConfiguration : ISourceConfiguration, ISinkConfiguration
    {
        var configuration = JsonConvert.DeserializeObject<TConfiguration>(SPEC.GetEnvironmentVariable());
        configureStreamConfiguration?.Invoke(configuration);
        return configuration;
    }
}
