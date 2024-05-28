using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Akka.Hosting;
using Akka.Util;
using Arcane.Framework.Contracts;
using Arcane.Framework.Services;
using Arcane.Framework.Services.Base;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Kubernetes.Providers;
using Snd.Sdk.Logs.Providers;
using Snd.Sdk.Logs.Providers.Configurations;

namespace Arcane.Framework.Providers.Hosting;

/// <summary>
/// Extension methods for building the console streaming host.
/// </summary>
public static class HostBuilderExtensions
{
    private const string ENV_PREFIX = "STREAMCONTEXT__";

    /// <summary>
    /// Add the default logging configuration to the streaming host builder.
    /// </summary>
    /// <param name="builder">IHostBuilder instance</param>
    /// <param name="configureLogger">Optional logger configuration callback.</param>
    /// <param name="contextBuilder">StreamingHostBuilderContext builder function</param>
    /// <returns>Configured IHostBuilder instance</returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IHostBuilder AddDatadogLogging(this IHostBuilder builder,
        Action<HostBuilderContext, IServiceProvider, LoggerConfiguration> configureLogger = null,
        Func<StreamingHostBuilderContext> contextBuilder = null)
    {
        var context = contextBuilder?.Invoke() ?? StreamingHostBuilderContext.FromEnvironment(ENV_PREFIX);
        return builder.AddSerilogLogger(context.ApplicationName,
            (hostBuilderContext, applicationName, loggerConfiguration) =>
            {
                configureLogger?.Invoke(hostBuilderContext, applicationName, loggerConfiguration);
                loggerConfiguration
                    .Enrich.WithProperty("streamId", context.StreamId)
                    .Enrich.WithProperty("streamKind", context.StreamKind)
                    .AddDatadog();
            }
        );
    }

    /// <summary>
    /// Adds the required services for the streaming host.
    /// </summary>
    /// <param name="builder">IHostBuilder instance</param>
    /// <param name="provideStreamGraphBuilder">The function that adds the stream graph builder to the services collection.</param>
    /// <param name="provideStreamRunnerService">
    /// The function that adds the stream runner service to the services collection.
    /// This parameter is optional. If omitted, the default implementation will be used.
    /// </param>
    /// <param name="provideStreamLifetimeService">
    /// The function that adds the stream lifetime service to the services collection.
    /// This parameter is optional. If omitted, the default implementation will be used.
    /// </param>
    /// <param name="configureActorSystem">
    /// The function that provides the custom configuration for the actor system.
    /// This parameter is optional. If omitted, the actor system will be configured with default settings provided by the SnD.Sdk library.
    /// </param>
    /// <param name="provideStreamHostContextBuilder">StreamingHostBuilderContext builder function</param>
    /// <returns>Configured IHostBuilder instance</returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IHostBuilder ConfigureRequiredServices(this IHostBuilder builder,
        Func<IServiceCollection, IServiceCollection> provideStreamGraphBuilder,
        Func<IServiceProvider, IStreamRunnerService> provideStreamRunnerService = null,
        Func<IServiceProvider, IStreamLifetimeService> provideStreamLifetimeService = null,
        Func<HostBuilderContext, StreamingHostBuilderContext, Action<AkkaConfigurationBuilder>> configureActorSystem = null,
        Func<StreamingHostBuilderContext> provideStreamHostContextBuilder = null
    )
    {
        var context = provideStreamHostContextBuilder?.Invoke() ?? StreamingHostBuilderContext.FromEnvironment(ENV_PREFIX);
        return builder.ConfigureServices((HostBuilderContext, services) =>
        {
            provideStreamGraphBuilder(services);
            services.AddKubernetes()
                .AddServiceWithOptionalFactory<IStreamRunnerService, StreamRunnerService>(provideStreamRunnerService)
                .AddServiceWithOptionalFactory<IStreamLifetimeService, StreamLifetimeService>(provideStreamLifetimeService)
                .AddLocalActorSystem(configureActorSystem?.Invoke(HostBuilderContext, context))
                .AddSingleton<IArcaneExceptionHandler, ArcaneExceptionHandler>();
        });
    }

    /// <summary>
    /// Allows to configure additional services for the streaming host (metrics, logging, storage writers etc.).
    /// </summary>
    /// <param name="builder">IHostBuilder instance</param>
    /// <param name="configureAdditionalServices">The function that adds services to the services collection.</param>
    /// <param name="provideStreamHostContextBuilder">StreamingHostBuilderContext builder function</param>
    /// <returns>Configured IHostBuilder instance</returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IHostBuilder ConfigureAdditionalServices(this IHostBuilder builder,
        Action<IServiceCollection, StreamingHostBuilderContext> configureAdditionalServices,
        Func<StreamingHostBuilderContext> provideStreamHostContextBuilder = null)
    {
        var context = provideStreamHostContextBuilder?.Invoke() ?? StreamingHostBuilderContext.FromEnvironment(ENV_PREFIX);
        return builder.ConfigureServices((_, services) =>
        {
            configureAdditionalServices.Invoke(services, context);
        });
    }

    /// <summary>
    /// This method adds the untyped stream graph builder to the services collection.
    /// Untyped stream graph builder is used when the stream context is not known at compile time.
    /// In this case the implementation of the stream graph builder should contain the logic for determining the stream context type.
    /// </summary>
    /// <param name="services">Services collection.</param>
    /// <param name="provideStreamContext">The factory function that provides the stream context instance.</param>
    /// <param name="provideStreamHostContextBuilder">StreamingHostBuilderContext builder function</param>
    /// <param name="provideStreamStatusService">StreamingHostBuilderContext builder function</param>
    /// <typeparam name="TStreamGraphBuilder">The stream graph builder type.</typeparam>
    /// <returns>Services collection</returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IServiceCollection AddStreamGraphBuilder<TStreamGraphBuilder>(this IServiceCollection services,
        Func<StreamingHostBuilderContext, IStreamContext> provideStreamContext,
        Func<StreamingHostBuilderContext> provideStreamHostContextBuilder = null,
        Func<IServiceProvider, IStreamStatusService> provideStreamStatusService = null)
        where TStreamGraphBuilder : class, IStreamGraphBuilder<IStreamContext>
    {
        var context = provideStreamHostContextBuilder?.Invoke() ?? StreamingHostBuilderContext.FromEnvironment(ENV_PREFIX);
        services.AddSingleton<IStreamGraphBuilder<IStreamContext>, TStreamGraphBuilder>();
        services.AddServiceWithOptionalFactory<IStreamStatusService, StreamStatusService>(provideStreamStatusService);
        services.AddSingleton(_ => provideStreamContext(context));
        return services;
    }

    /// <summary>
    /// This method adds the typed stream graph builder to the services collection.
    /// The typed stream graph builder is used when the stream context type is known at compile time.
    /// I's preferable to use this method when possible.
    /// </summary>
    /// <param name="services">Services collection.</param>
    /// <param name="provideStreamHostContextBuilder">Provides a TStreamContext instance</param>
    /// <param name="provideStreamStatusService">Provides a TStreamContext instance</param>
    /// <typeparam name="TStreamContext">The stream context type</typeparam>
    /// <typeparam name="TStreamGraphBuilder">The stream graph builder type.</typeparam>
    /// <returns></returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IServiceCollection AddStreamGraphBuilder<TStreamGraphBuilder, TStreamContext>(this IServiceCollection services,
        Func<TStreamContext> provideStreamHostContextBuilder = null,
        Func<IServiceProvider, IStreamStatusService> provideStreamStatusService = null)
        where TStreamContext : class, IStreamContext, IStreamContextWriter, new()
        where TStreamGraphBuilder : class, IStreamGraphBuilder<TStreamContext>
    {
        services.AddSingleton<IStreamGraphBuilder<TStreamContext>, TStreamGraphBuilder>();
        services.AddServiceWithOptionalFactory<IStreamStatusService, StreamStatusService>(provideStreamStatusService);
        services.AddStreamContext<TStreamContext>(provideStreamHostContextBuilder);
        return services;
    }

    /// <summary>
    /// Runs the stream created by the typed stream graph builder.
    /// </summary>
    /// <param name="host">Streaming application host</param>
    /// <param name="logger">Static bootstrap logger</param>
    /// <param name="handleUnknownException">
    /// Exception handler for unhandled exceptions.
    /// If omitted, the default handler will be used.
    /// If provided, this handler will be invoked after the default handler.
    /// The handler should return the application exit code.
    /// </param>
    /// <typeparam name="TContext">
    /// The stream context type. This should match the type that uses injected implementation if the
    /// IStreamGraphBuilder interface.
    /// </typeparam>
    /// <returns>Application exit code</returns>
    public static async Task<int> RunStream<TContext>(this IHost host, ILogger logger, Func<Exception, ILogger, Task<Option<int>>> handleUnknownException = null)
    where TContext : IStreamContext
    {
        var runner = host.Services.GetRequiredService<IStreamRunnerService>();
        var exceptionHandler = host.Services.GetService<IArcaneExceptionHandler>();
        var context = host.Services.GetRequiredService<TContext>();
        using var lifetimeService = host.Services.GetRequiredService<IStreamLifetimeService>();
        var graphBuilder = host.Services.GetRequiredService<IStreamGraphBuilder<TContext>>();
        try
        {
            var completeTask = runner.RunStream(() => graphBuilder.BuildGraph(context));
            await completeTask;
        }
        catch (Exception e)
        {
            if (exceptionHandler is null)
            {
                return await TryHandleUnknownException(e, logger, handleUnknownException);
            }
            var handled = await exceptionHandler.HandleException(e);
            return handled switch
            {

                { HasValue: true, Value: var exitCode } => exitCode,
                { HasValue: false } => await TryHandleUnknownException(e, logger, handleUnknownException),
            };
        }

        logger.Information("Streaming job is completed successfully, exiting");
        return ExitCodes.SUCCESS;
    }

    /// <summary>
    /// Runs the stream created by the untyped stream graph builder.
    /// </summary>
    /// <param name="host">Streaming application host</param>
    /// <param name="logger">Static bootstrap logger</param>
    /// <param name="handleUnknownException">
    /// Exception handler for unhandled exceptions.
    /// If omitted, the default handler will be used.
    /// If provided, this handler will be invoked after the default handler.
    /// The handler should return the application exit code.
    /// </param>
    /// <returns>Application exit code</returns>
    public static async Task<int> RunStream(this IHost host, ILogger logger,
        Func<Exception, ILogger, Task<Option<int>>> handleUnknownException = null)
    {
        return await RunStream<IStreamContext>(host, logger, handleUnknownException);
    }

    private static async Task<int> TryHandleUnknownException(Exception e, ILogger logger, Func<Exception, ILogger, Task<Option<int>>> handleUnknownException = null)
    {
        if (handleUnknownException is null)
        {
            return FatalExit(e, logger);
        }
        return await handleUnknownException(e, logger) switch
        {
            { HasValue: true, Value: var exitCode } => exitCode,
            _ => FatalExit(e, logger),
        };
    }

    private static int FatalExit(Exception e, ILogger logger)
    {
        logger.Error(e, "Unhandled exception occurred");
        return ExitCodes.FATAL;
    }

    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    private static IServiceCollection AddServiceWithOptionalFactory<TService, TImplementation>(this IServiceCollection services, Func<IServiceProvider, TService> factory = null)
        where TService : class where TImplementation : class, TService
    {
        if (factory != null)
        {
            services.AddSingleton(factory);
        }
        else
        {
            services.AddSingleton<TService, TImplementation>();
        }
        return services;
    }
}
