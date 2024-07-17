using System.Collections.Generic;
using System.Text.Json;
using Serilog;
using SnD.Sdk.Extensions.Environment.Hosting;

namespace Arcane.Framework.Contracts;

/// <summary>
/// Logging extension methods for Arcane framework.
/// </summary>
public static class Logging
{
    /// <summary>
    /// Allows enriching the logger with properties from the environment.
    /// </summary>
    /// <param name="configuration"></param>
    /// <returns></returns>
    public static LoggerConfiguration EnrichWithCustomProperties(this LoggerConfiguration configuration)
    {
        var loggingProperties = EnvironmentExtensions.GetDomainEnvironmentVariable("LOGGING_PROPERTIES");
        if (string.IsNullOrEmpty(loggingProperties))
        {
            return configuration;
        }

        var properties = JsonSerializer.Deserialize<Dictionary<string, string>>(loggingProperties);
        foreach (var (key, value) in properties)
        {
            configuration.Enrich.WithProperty(key, value);
        }

        return configuration;
    }
}
