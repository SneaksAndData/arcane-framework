using System;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Services.Extensions;

namespace Arcane.Framework.Services;

/// <summary>
/// Provides access to a streaming job configuration
/// </summary>
public class StreamConfigurationManager: IStreamConfigurationManager
{
    private const string STREAM_KIND = nameof(STREAM_KIND);
    private const string STREAM_ID = nameof(STREAM_ID);
    private const string FULL_LOAD = nameof(FULL_LOAD);

    public string StreamId => STREAM_ID.GetEnvironmentVariable();
    public string StreamKind => STREAM_KIND.GetEnvironmentVariable();

    public bool IsRunningInBackfillMode  => FULL_LOAD.GetEnvironmentVariable().Equals("true", StringComparison.OrdinalIgnoreCase);
}
