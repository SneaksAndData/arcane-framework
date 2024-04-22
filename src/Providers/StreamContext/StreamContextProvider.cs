using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Arcane.Framework.Extensions;
using Arcane.Framework.Services.Base;
using Microsoft.Extensions.DependencyInjection;
using Snd.Sdk.Hosting;

namespace Arcane.Framework.Providers.StreamContext;

/// <summary>
/// Provider for the stream context
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public static class StreamContextProvider
{
    /// <summary>
    /// Adds a stream context instance to DI container as a singleton
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="provider"></param>
    /// <typeparam name="TStreamContext"></typeparam>
    /// <returns></returns>
    public static IServiceCollection AddStreamContext<TStreamContext>(this IServiceCollection services, Func<TStreamContext> provider = null)
        where TStreamContext : class, IStreamOptions, new()
    {
        var context = provider?.Invoke() ?? ProvideFromEnvironment<TStreamContext>();
        services.AddSingleton<IStreamContext>(context);
        return services;
    }

    /// <summary>
    /// Stream context provider that uses the Environment variables for populating IStreamContext properties
    /// </summary>
    /// <typeparam name="TStreamContext">Stream context type</typeparam>
    /// <returns>Stream context instance</returns>
    public static TStreamContext ProvideFromEnvironment<TStreamContext>() where TStreamContext : class, IStreamOptions
    {
        var context = JsonSerializer.Deserialize<TStreamContext>(EnvironmentExtensions.GetAssemblyEnvironmentVariable("SPEC"));
        context.IsRunningInBackfillMode = EnvironmentExtensions.GetAssemblyEnvironmentVariable("FULL_LOAD")
            .Equals("true", System.StringComparison.InvariantCultureIgnoreCase);;
        context.StreamId = EnvironmentExtensions.GetAssemblyEnvironmentVariable("STREAM_ID");
        context.StreamKind = EnvironmentExtensions.GetAssemblyEnvironmentVariable("STREAM_KIND");
        return context;
    }
}

