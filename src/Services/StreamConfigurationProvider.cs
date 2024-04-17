using Arcane.Framework.Services.Base;
using Arcane.Framework.Services.Extensions;

namespace Arcane.Framework.Services;

/// <summary>
/// Provides access to a streaming job configuration
/// </summary>
public class StreamConfigurationProvider: IStreamConfigurationProvider
{
    /// <summary>
    /// Stream kind environment variable name
    /// </summary>
    private const string STREAM_KIND = nameof(STREAM_KIND);

    /// <summary>
    /// Returns stream kind from environment
    /// </summary>
    /// <returns></returns>
    public string GetStreamKind()
    {
        return STREAM_KIND.GetEnvironmentVariable();
    }
}
