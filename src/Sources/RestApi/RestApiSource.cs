using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Stage;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Framework.Contracts;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Sources.Base;
using Arcane.Framework.Sources.Extensions;
using Arcane.Framework.Sources.RestApi.Extensions;
using Arcane.Framework.Sources.RestApi.Services.AuthenticatedMessageProviders;
using Arcane.Framework.Sources.RestApi.Services.AuthenticatedMessageProviders.Base;
using Arcane.Framework.Sources.RestApi.Services.UriProviders;
using Arcane.Framework.Sources.RestApi.Services.UriProviders.Base;
using Microsoft.OpenApi.Models;
using Parquet.Data;
using Polly.RateLimit;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Sources.RestApi;

/// <summary>
/// Source for reading data from a REST API.
/// </summary>
public class RestApiSource : GraphStage<SourceShape<JsonElement>>, IParquetSource, ITaggedSource
{
    private readonly IRestApiAuthenticatedMessageProvider _authenticatedMessageProvider;
    private readonly OpenApiSchema apiSchema;
    private readonly TimeSpan changeCaptureInterval;
    private readonly bool fullLoadOnStart;
    private readonly HttpClient httpClient;
    private readonly TimeSpan httpRequestTimeout;
    private readonly TimeSpan lookBackInterval;
    private readonly AsyncRateLimitPolicy rateLimitPolicy;
    private readonly string[] responsePropertyKeyChain;
    private readonly bool stopAfterFullLoad;
    private readonly string streamKind;
    private readonly IRestApiUriProvider uriProvider;

    private RestApiSource(
        IRestApiUriProvider uriProvider,
        IRestApiAuthenticatedMessageProvider authenticatedMessageProvider,
        bool fullLoadOnStart,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        bool stopAfterFullLoad,
        string streamKind,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        this.uriProvider = uriProvider;
        this.stopAfterFullLoad = stopAfterFullLoad;
        this.changeCaptureInterval = changeCaptureInterval;
        this.fullLoadOnStart = fullLoadOnStart;
        this._authenticatedMessageProvider = authenticatedMessageProvider;
        this.lookBackInterval = lookBackInterval;
        this.streamKind = streamKind;
        this.rateLimitPolicy = rateLimitPolicy;
        this.responsePropertyKeyChain = responsePropertyKeyChain;
        this.apiSchema = apiSchema;

        this.Shape = new SourceShape<JsonElement>(this.Out);
    }

    private RestApiSource(
        IRestApiUriProvider uriProvider,
        IRestApiAuthenticatedMessageProvider authenticatedMessageProvider,
        bool fullLoadOnStart,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterFullLoad,
        string streamKind,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null) : this(uriProvider, authenticatedMessageProvider, fullLoadOnStart,
        changeCaptureInterval, lookBackInterval, stopAfterFullLoad, streamKind, rateLimitPolicy, apiSchema,
        responsePropertyKeyChain)
    {
        this.httpRequestTimeout = httpRequestTimeout;
    }

    /// <summary>
    /// Only use this constructor for unit tests to mock http calls.
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="authenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="httpClient">Http client for making requests</param>
    /// <param name="streamKind">Stream kind</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="fullLoadOnStart">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterFullLoad">Set to true if stream should stop after full load is finished</param>
    private RestApiSource(
        IRestApiUriProvider uriProvider,
        IRestApiAuthenticatedMessageProvider authenticatedMessageProvider,
        bool fullLoadOnStart,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        HttpClient httpClient,
        bool stopAfterFullLoad,
        string streamKind,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null) : this(uriProvider, authenticatedMessageProvider, fullLoadOnStart,
        changeCaptureInterval, lookBackInterval, stopAfterFullLoad, streamKind, rateLimitPolicy, apiSchema,
        responsePropertyKeyChain)
    {
        this.httpClient = httpClient;
    }

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(RestApiSource));

    /// <summary>
    /// Source outlet
    /// </summary>
    public Outlet<JsonElement> Out { get; } = new($"{nameof(RestApiSource)}.Out");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SourceShape<JsonElement> Shape { get; }

    /// <inheritdoc cref="IParquetSource.GetParquetSchema"/>
    public Schema GetParquetSchema()
    {
        return this.apiSchema.ToParquetSchema();
    }

    /// <inheritdoc cref="ITaggedSource.GetDefaultTags"/>
    public SourceTags GetDefaultTags()
    {
        return new SourceTags
        {
            SourceEntity = this.uriProvider.BaseUri.AbsolutePath,
            SourceLocation = this.uriProvider.BaseUri.Host,
            StreamKind = this.streamKind
        };
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="streamKind">Stream kind</param>
    /// <param name="httpRequestTimeout">Http request rimeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="fullLoadOnStart">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterFullLoad">Set to true if stream should stop after full load is finished</param>
    /// <param name="headerAuthenticatedMessageProvider">Authenticated message provider</param>
    public static RestApiSource Create(
        SimpleUriProvider uriProvider,
        FixedHeaderAuthenticatedMessageProvider headerAuthenticatedMessageProvider,
        bool fullLoadOnStart,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterFullLoad,
        string streamKind,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema)
    {
        return new RestApiSource(uriProvider, headerAuthenticatedMessageProvider, fullLoadOnStart,
            changeCaptureInterval,
            lookBackInterval, httpRequestTimeout, stopAfterFullLoad, streamKind, rateLimitPolicy, apiSchema);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="streamKind">Stream kind</param>
    /// <param name="httpClient">Http Client</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="fullLoadOnStart">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterFullLoad">Set to true if stream should stop after full load is finished</param>
    /// <param name="headerAuthenticatedMessageProvider">Authenticated message provider</param>
    public static RestApiSource Create(
        SimpleUriProvider uriProvider,
        FixedHeaderAuthenticatedMessageProvider headerAuthenticatedMessageProvider,
        bool fullLoadOnStart,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        HttpClient httpClient,
        bool stopAfterFullLoad,
        string streamKind,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema)
    {
        return new RestApiSource(uriProvider, headerAuthenticatedMessageProvider, fullLoadOnStart,
            changeCaptureInterval,
            lookBackInterval, httpClient, stopAfterFullLoad, streamKind, rateLimitPolicy, apiSchema);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="streamKind">Stream kind</param>
    /// <param name="httpRequestTimeout">Http request timeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="fullLoadOnStart">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterFullLoad">Set to true if stream should stop after full load is finished</param>
    /// <param name="authHeaderAuthenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    public static RestApiSource Create(
        PagedUriProvider uriProvider,
        DynamicBearerAuthenticatedMessageProvider authHeaderAuthenticatedMessageProvider,
        bool fullLoadOnStart,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterFullLoad,
        string streamKind,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        return new RestApiSource(uriProvider, authHeaderAuthenticatedMessageProvider, fullLoadOnStart,
            changeCaptureInterval,
            lookBackInterval, httpRequestTimeout, stopAfterFullLoad, streamKind, rateLimitPolicy, apiSchema,
            responsePropertyKeyChain);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="streamKind">Stream kind</param>
    /// <param name="httpRequestTimeout">Http request timeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="fullLoadOnStart">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterFullLoad">Set to true if stream should stop after full load is finished</param>
    /// <param name="headerAuthenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    public static RestApiSource Create(
        PagedUriProvider uriProvider,
        FixedHeaderAuthenticatedMessageProvider headerAuthenticatedMessageProvider,
        bool fullLoadOnStart,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterFullLoad,
        string streamKind,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        return new RestApiSource(uriProvider, headerAuthenticatedMessageProvider, fullLoadOnStart,
            changeCaptureInterval,
            lookBackInterval, httpRequestTimeout, stopAfterFullLoad, streamKind, rateLimitPolicy, apiSchema,
            responsePropertyKeyChain);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="streamKind">Stream kind</param>
    /// <param name="httpClient">Http request timeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="fullLoadOnStart">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterFullLoad">Set to true if stream should stop after full load is finished</param>
    /// <param name="authHeaderAuthenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    public static RestApiSource Create(
        PagedUriProvider uriProvider,
        DynamicBearerAuthenticatedMessageProvider authHeaderAuthenticatedMessageProvider,
        bool fullLoadOnStart,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        HttpClient httpClient,
        bool stopAfterFullLoad,
        string streamKind,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        return new RestApiSource(uriProvider, authHeaderAuthenticatedMessageProvider, fullLoadOnStart,
            changeCaptureInterval,
            lookBackInterval, httpClient, stopAfterFullLoad, streamKind, rateLimitPolicy, apiSchema,
            responsePropertyKeyChain);
    }

    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new SourceLogic(this);
    }

    private sealed class SourceLogic : TimerGraphStageLogic, IStopAfterBackfill
    {
        private const string TimerKey = nameof(RestApiSource);
        private readonly LocalOnlyDecider decider;
        private readonly HttpClient httpClient;
        private readonly RestApiSource source;
        private HttpResponseMessage currentResponse;

        private Action<Task<Option<JsonElement>>> responseReceived;

        public SourceLogic(RestApiSource source) : base(source.Shape)
        {
            this.source = source;

            this.httpClient = this.source.httpClient ?? new HttpClient
            {
                Timeout = this.source.httpRequestTimeout
            };

            this.decider = Decider.From((ex) => ex.GetType().Name switch
            {
                nameof(ArgumentException) => Directive.Stop,
                nameof(ArgumentNullException) => Directive.Stop,
                nameof(InvalidOperationException) => Directive.Stop,
                nameof(ConfigurationErrorsException) => Directive.Stop,
                nameof(ObjectDisposedException) => Directive.Stop,
                nameof(IOException) => Directive.Restart,
                nameof(TimeoutException) => Directive.Restart,
                nameof(HttpRequestException) => Directive.Restart,
                _ => Directive.Stop
            });


            this.SetHandler(source.Out, this.PullChanges, this.Finish);
        }

        /// <inheritdoc cref="IStopAfterBackfill.IsRunningInBackfillMode"/>
        public bool IsRunningInBackfillMode { get; set; }

        /// <inheritdoc cref="IStopAfterBackfill.StopAfterBackfill"/>
        public bool StopAfterBackfill => this.source.stopAfterFullLoad;

        private void Finish(Exception cause)
        {
            if (cause is not null && cause is not SubscriptionWithCancelException.NonFailureCancellation)
            {
                this.FailStage(cause);
            }

            this.httpClient.Dispose();
        }

        public override void PreStart()
        {
            base.PreStart();

            this.responseReceived = this.GetAsyncCallback<Task<Option<JsonElement>>>(this.OnRecordReceived);

            if (this.source.fullLoadOnStart)
            {
                this.IsRunningInBackfillMode = true;
            }
        }

        private void OnRecordReceived(Task<Option<JsonElement>> readTask)
        {
            if (readTask.IsFaulted || readTask.IsCanceled)
            {
                switch (this.decider.Decide(readTask.Exception))
                {
                    case Directive.Stop:
                        this.Finish(readTask.Exception);
                        break;
                    default:
                        this.ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
                        break;
                }

                return;
            }

            var taskResult = readTask.Result
                .GetOrElse(JsonSerializer.Deserialize<JsonElement>(
                    (this.source.responsePropertyKeyChain ?? Array.Empty<string>()).Any() ? "{}" : "[]"))
                .ParseResponse(this.source.responsePropertyKeyChain).ToList();

            // Current batch has ended, start a new one
            if (!taskResult.Any())
            {
                if (((this.source.uriProvider as IPaginatedApiUriProvider)?.HasReadAllPages())
                    .GetValueOrDefault(true) &&
                    this.CompleteStageAfterFullLoad(this.Finish))
                {
                    this.Log.Info("No data returned by latest call and all pages have been downloaded");
                    return;
                }

                this.Log.Info(
                    $"No data returned by latest call, next check in {this.source.changeCaptureInterval.TotalSeconds} seconds");

                this.ScheduleOnce(TimerKey, this.source.changeCaptureInterval);
            }
            else
            {
                this.EmitMultiple(this.source.Out, taskResult);
            }
        }

        private void PullChanges()
        {
            this.source.rateLimitPolicy.ExecuteAsync(() => this.source._authenticatedMessageProvider
                .GetAuthenticatedMessage(this.httpClient)
                .Map(msg =>
                {
                    this.Log.Debug("Successfully authenticated");
                    var (maybeNextUri, requestMethod, maybePayload) = this.source.uriProvider.GetNextResultUri(
                        this.currentResponse, this.IsRunningInBackfillMode, this.source.lookBackInterval,
                        this.source.changeCaptureInterval);

                    if (maybeNextUri.IsEmpty)
                    {
                        this.currentResponse = null;
                        return Task.FromResult(Option<JsonElement>.None);
                    }

                    msg.RequestUri = maybeNextUri.Value;
                    msg.Method = requestMethod;

                    if (maybePayload.HasValue)
                    {
                        msg.Content = new StringContent(maybePayload.Value);
                        this.Log.Info($"Request payload for next result: {maybePayload.Value}");
                    }

                    this.Log.Info($"Requesting next result from {msg.RequestUri}");

                    return this.httpClient.SendAsync(msg, default(CancellationToken))
                        .Map(response =>
                        {
                            if (response.IsSuccessStatusCode)
                            {
                                this.currentResponse = response;
                                return response.Content.ReadAsStringAsync().Map(value =>
                                {
                                    this.Log.Debug($"Got response: {value}");
                                    return JsonSerializer.Deserialize<JsonElement>(value).AsOption();
                                });
                            }

                            var errorMsg =
                                $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

                            this.Log.Warning(errorMsg);

                            throw new HttpRequestException(errorMsg, null, response.StatusCode);
                        }).Flatten();
                }).Flatten()).TryMap(result => result, exception => exception switch
            {
                RateLimitRejectedException => Option<JsonElement>.None, // configured rate limit
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.TooManyRequests
                } => Option<JsonElement>.None, // API rate limit, in case configured rate limit is not good enough
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.RequestTimeout
                } => Option<JsonElement>.None, // Potential server-side timeout due to overload
                _ => throw exception
            }).ContinueWith(this.responseReceived);
        }

        protected override void OnTimer(object timerKey)
        {
            this.PullChanges();
        }
    }
}
