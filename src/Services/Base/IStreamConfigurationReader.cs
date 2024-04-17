using System;
using Newtonsoft.Json;

namespace Arcane.Framework.Services.Base;

/// <summary>
/// Provides common streaming job configuration properties
/// </summary>
public interface IStreamConfigurationReader
{
    /// <summary>
    /// Id of the stream
    /// </summary>
    public string StreamId { get; }

    /// <summary>
    /// True if stream is running in backfill (full reload) mode
    /// </summary>
    public bool IsRunningInBackfillMode { get; }

    public string StreamKind { get; }

    public TConfiguration Read<TConfiguration>(Action<TConfiguration> configureStreamConfiguration = null)
        where TConfiguration : class, new();
}
