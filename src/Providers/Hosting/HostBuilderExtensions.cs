using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Akka.Hosting;
using Akka.Util;
using Arcane.Framework.Contracts;
using Arcane.Framework.Services;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Sources.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Snd.Sdk.ActorProviders;
using Snd.Sdk.Kubernetes.Providers;

namespace Arcane.Framework.Providers.Hosting;

/// <summary>
/// Extension methods for building the console streaming host.
/// </summary>
public static class HostBuilderExtensions
{
    /// <summary>
    /// Adds the required services for the streaming host.
    /// </summary>
    /// <param name="builder">IHostBuilder instance</param>
    /// <param name="addStreamGraphBuilder">The function that adds the stream graph builder to the services collection.</param>
    /// <param name="addStreamRunnerService">
    /// The function that adds the stream runner service to the services collection.
    /// This parameter is optional. If omitted, the default implementation will be used.
    /// </param>
    /// <param name="addStreamLifetimeService">
    /// The function that adds the stream lifetime service to the services collection.
    /// This parameter is optional. If omitted, the default implementation will be used.
    /// </param>
    /// <param name="configureActorSystem">
    /// The function that provides the custom configuration for the actor system.
    /// This parameter is optional. If omitted, the actor system will be configured with default settings provided by the SnD.Sdk library.
    /// </param>
    /// <returns>Configured IHostBuilder instance</returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IHostBuilder ConfigureRequiredServices(this IHostBuilder builder,
        Func<IServiceCollection, IServiceCollection> addStreamGraphBuilder,
        Func<IServiceProvider, IStreamRunnerService> addStreamRunnerService = null,
        Func<IServiceProvider, IStreamLifetimeService> addStreamLifetimeService = null,
        Func<HostBuilderContext, StreamingHostBuilderContext, Action<AkkaConfigurationBuilder>> configureActorSystem = null
    )
    {
        return builder.ConfigureServices((HostBuilderContext, services) =>
        {
            addStreamGraphBuilder(services);
            services.AddKubernetes()
                .AddServiceWithOptionalFactory<IStreamRunnerService, StreamRunnerService>(addStreamRunnerService)
                .AddServiceWithOptionalFactory<IStreamLifetimeService, StreamLifetimeService>(addStreamLifetimeService)
                .AddLocalActorSystem(configureActorSystem?.Invoke(HostBuilderContext, StreamingHostBuilderContext.FromEnvironment()));
        });
    }

    /// <summary>
    /// Allows to configure additional services for the streaming host (metrics, logging, storage writers etc.).
    /// </summary>
    /// <param name="builder">IHostBuilder instance</param>
    /// <param name="configureAdditionalServices">The function that adds services to the services collection.</param>
    /// <returns>Configured IHostBuilder instance</returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IHostBuilder ConfigureAdditionalServices(this IHostBuilder builder,
        Action<IServiceCollection, StreamingHostBuilderContext> configureAdditionalServices)
    {
        return builder.ConfigureServices((_, services) =>
        {
            configureAdditionalServices.Invoke(services, StreamingHostBuilderContext.FromEnvironment());
        });
    }

    /// <summary>
    /// This method adds the untyped stream graph builder to the services collection.
    /// Untyped stream graph builder is used when the stream context is not known at compile time.
    /// In this case the implementation of the stream graph builder should contain the logic for determining the stream context type.
    /// </summary>
    /// <param name="services">Services collection.</param>
    /// <param name="provideStreamContext">The factory function that provides the stream context instance.</param>
    /// <typeparam name="TStreamGraphBuilder">The stream graph builder type.</typeparam>
    /// <returns>Services collection</returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IServiceCollection AddStreamGraphBuilder<TStreamGraphBuilder>(this IServiceCollection services,
        Func<StreamingHostBuilderContext, IStreamContext> provideStreamContext)
        where  TStreamGraphBuilder : class, IStreamGraphBuilder<IStreamContext>
        {
            var context = new StreamingHostBuilderContext();
            services.AddSingleton<IStreamGraphBuilder<IStreamContext>, TStreamGraphBuilder>();
            services.AddSingleton<IStreamStatusService, StreamStatusService>();
            services.AddSingleton(_ => provideStreamContext(context));
            return services;
        }

    /// <summary>
    /// This method adds the typed stream graph builder to the services collection.
    /// The typed stream graph builder is used when the stream context type is known at compile time.
    /// I's preferable to use this method when possible.
    /// </summary>
    /// <param name="services">Services collection.</param>
    /// <typeparam name="TStreamContext">The stream context type</typeparam>
    /// <typeparam name="TStreamGraphBuilder">The stream graph builder type.</typeparam>
    /// <returns></returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IServiceCollection AddStreamGraphBuilder<TStreamGraphBuilder, TStreamContext>(this IServiceCollection services)
        where TStreamContext : class, IStreamContext, IStreamContextWriter, new() where TStreamGraphBuilder : class, IStreamGraphBuilder<TStreamContext>
        {
            services.AddSingleton<IStreamGraphBuilder<TStreamContext>, TStreamGraphBuilder>();
            services.AddSingleton<IStreamStatusService, StreamStatusService>();
            services.AddStreamContext<TStreamContext>();
            return services;
        }

    /// <summary>
    /// Adds the default exception handler that handles exceptions thrown by the stream runner.
    /// Handlers handles the following exceptions:
    /// * <see cref="SchemaMismatchException"/> In this case stream runner will request data backfill.
    /// * <see cref="SchemaInconsistentException"/> In this case stream runner will request restart without
    ///     incrementing the retry counter of the Kubernetes Job that controls the stream runner.
    /// </summary>
    /// <param name="services">Services collection.</param>
    /// <returns></returns>
    [ExcludeFromCodeCoverage(Justification = "Trivial")]
    public static IServiceCollection AddDefaultExceptionHandler(this IServiceCollection services) =>
        services.AddSingleton<IArcaneExceptionHandler, ArcaneExceptionHandler>();

    /// <summary>
    /// Runs the stream using the provided stream runner service.
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
    public static async Task<int> RunStream(this IHost host, ILogger logger, Func<Exception, Task<Option<int>>> handleUnknownException = null)
    {
        var runner = host.Services.GetRequiredService<IStreamRunnerService>();
        var exceptionHandler = host.Services.GetService<IArcaneExceptionHandler>();
        var context = host.Services.GetRequiredService<IStreamContext>();
        var graphBuilder = host.Services.GetRequiredService<IStreamGraphBuilder<IStreamContext>>();
        try
        {
            var completeTask = runner.RunStream(() => graphBuilder.BuildGraph(context));
            await completeTask;
        }
        catch (Exception e)
        {
            if (exceptionHandler is null)
            {
                if(handleUnknownException is null)
                {
                    return FatalExit(e, logger);
                }
                return await handleUnknownException(e) switch
                {
                    {HasValue: true, Value: var exitCode} => exitCode,
                    _ => FatalExit(e, logger),
                };
            }
            var handled = await exceptionHandler?.HandleException(e);
            return handled switch
            {

                { HasValue: true, Value: var exitCode } => exitCode,
                { HasValue: false } => FatalExit(e, logger),
            };
        }

        logger.Information("Streaming job is completed successfully, exiting");
        return ExitCodes.SUCCESS;
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
