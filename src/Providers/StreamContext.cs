using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Arcane.Framework.Providers.Hosting;
using Arcane.Framework.Services.Base;
using Microsoft.Extensions.DependencyInjection;

namespace Arcane.Framework.Providers;

/// <summary>
/// Provider for the stream context
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public static class StreamContext
{
    private const string ENV_PREFIX = "STREAMCONTEXT__";
    private static JsonSerializerOptions DeFaultOptions => new() { PropertyNameCaseInsensitive = true };

    /// <summary>
    /// Adds a stream context instance to DI container as a singleton
    /// </summary>
    /// <param name="services">Service collection</param>
    /// <param name="provider">Crates a new StreamContext instance (defaults to <see cref="ProvideFromEnvironment"/></param>
    /// <typeparam name="TStreamContext">Type of the StreamContext</typeparam>
    /// <returns></returns>
    public static IServiceCollection AddStreamContext<TStreamContext>(this IServiceCollection services,
        Func<TStreamContext> provider = null)
        where TStreamContext : class, IStreamContextWriter, IStreamContext, new()
    {
        var context = provider?.Invoke() ?? ProvideFromEnvironment<TStreamContext>();

        // injection for IStreamGraphBuilder
        services.AddSingleton(context);

        // injection for other services that can use IStreamContext
        services.AddSingleton<IStreamContext>(context);
        return services;
    }

    /// <summary>
    /// Stream context provider that uses the Environment variables for populating IStreamContext properties
    /// </summary>
    /// <typeparam name="TStreamContext">Stream context type</typeparam>
    /// <returns>Stream context instance</returns>
    public static TStreamContext ProvideFromEnvironment<TStreamContext>()
        where TStreamContext : class, IStreamContextWriter, IStreamContext
    {
        var streamSpec = Environment.GetEnvironmentVariable($"{ENV_PREFIX}SPEC") ??
                        throw new ArgumentNullException($"{ENV_PREFIX}SPEC");
        var context = JsonSerializer.Deserialize<TStreamContext>(streamSpec, DeFaultOptions);
        PopulateStreamContext(context);
        return context;
    }

    /// <summary>
    /// Stream context provider that uses the Environment variables for populating IStreamContext properties.
    /// </summary>
    /// <param name="targetType">
    /// Target type to deserialize. The type must
    /// implement <see cref="IStreamContext"/> and <see cref="IStreamContextWriter"/> interfaces.
    /// </param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static IStreamContext ProvideFromEnvironment(Type targetType)
    {
        var streamSpec = Environment.GetEnvironmentVariable($"{ENV_PREFIX}SPEC") ??
                        throw new ArgumentNullException($"{ENV_PREFIX}SPEC");
        var context = JsonSerializer.Deserialize(streamSpec, targetType, DeFaultOptions);
        if (context is IStreamContextWriter contextWriter)
        {
            PopulateStreamContext(contextWriter);
            if (context is IStreamContext streamContext)
            {
                return streamContext;
            }
        }
        throw new InvalidOperationException(
            $"Stream context type {targetType} should implement {nameof(IStreamContextWriter)}, {nameof(IStreamContext)}");
    }

    private static void PopulateStreamContext(IStreamContextWriter contextWriter)
    {
        var streamBuilderContext = StreamingHostBuilderContext.FromEnvironment(ENV_PREFIX);
        contextWriter.SetBackfilling(streamBuilderContext.IsBackfilling);
        contextWriter.SetStreamId(streamBuilderContext.StreamId);
        contextWriter.SetStreamKind(streamBuilderContext.StreamKind);
    }
}

