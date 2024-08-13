using System;
using System.Diagnostics.CodeAnalysis;
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
using Google.Protobuf.WellKnownTypes;
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
    private readonly IRestApiAuthenticatedMessageProvider authenticatedMessageProvider;
    private readonly OpenApiSchema apiSchema;
    private readonly TimeSpan changeCaptureInterval;
    private readonly bool isBackfilling;
    private readonly HttpClient httpClient;
    private readonly TimeSpan httpRequestTimeout;
    private readonly TimeSpan lookBackInterval;
    private readonly AsyncRateLimitPolicy rateLimitPolicy;
    private readonly string[] responsePropertyKeyChain;
    private readonly bool stopAfterBackfill;
    private readonly IRestApiUriProvider uriProvider;

    private RestApiSource(
        IRestApiUriProvider uriProvider,
        IRestApiAuthenticatedMessageProvider authenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        this.uriProvider = uriProvider;
        this.stopAfterBackfill = stopAfterBackfill;
        this.changeCaptureInterval = changeCaptureInterval;
        this.isBackfilling = isBackfilling;
        this.authenticatedMessageProvider = authenticatedMessageProvider;
        this.lookBackInterval = lookBackInterval;
        this.rateLimitPolicy = rateLimitPolicy;
        this.responsePropertyKeyChain = responsePropertyKeyChain;
        this.apiSchema = apiSchema;

        this.Shape = new SourceShape<JsonElement>(this.Out);
    }

    private RestApiSource(
        IRestApiUriProvider uriProvider,
        IRestApiAuthenticatedMessageProvider authenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null) : this(uriProvider, authenticatedMessageProvider, isBackfilling,
        changeCaptureInterval, lookBackInterval, stopAfterBackfill, rateLimitPolicy, apiSchema,
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
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="isBackfilling">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterBackfill">Set to true if stream should stop after full load is finished</param>
    private RestApiSource(
        IRestApiUriProvider uriProvider,
        IRestApiAuthenticatedMessageProvider authenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        HttpClient httpClient,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null) : this(uriProvider, authenticatedMessageProvider, isBackfilling,
        changeCaptureInterval, lookBackInterval, stopAfterBackfill, rateLimitPolicy, apiSchema,
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
    public Schema GetParquetSchema() => this.apiSchema.ToParquetSchema();

    /// <inheritdoc cref="ITaggedSource.GetDefaultTags"/>
    public SourceTags GetDefaultTags()
    {
        return new SourceTags
        {
            SourceEntity = this.uriProvider.BaseUri.AbsolutePath,
            SourceLocation = this.uriProvider.BaseUri.Host,
        };
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="httpRequestTimeout">Http request rimeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="isBackfilling">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterBackfill">Set to true if stream should stop after full load is finished</param>
    /// <param name="headerAuthenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static RestApiSource Create(
        SimpleUriProvider uriProvider,
        FixedHeaderAuthenticatedMessageProvider headerAuthenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        return new RestApiSource(uriProvider, headerAuthenticatedMessageProvider, isBackfilling,
            changeCaptureInterval,
            lookBackInterval, httpRequestTimeout, stopAfterBackfill, rateLimitPolicy, apiSchema,
            responsePropertyKeyChain);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="httpRequestTimeout">Http request rimeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="isBackfilling">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterBackfill">Set to true if stream should stop after full load is finished</param>
    /// <param name="headerAuthenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static RestApiSource Create(
        SimpleUriProvider uriProvider,
        DynamicBearerAuthenticatedMessageProvider headerAuthenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        return new RestApiSource(uriProvider, headerAuthenticatedMessageProvider, isBackfilling,
            changeCaptureInterval,
            lookBackInterval, httpRequestTimeout, stopAfterBackfill, rateLimitPolicy, apiSchema,
            responsePropertyKeyChain);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="httpClient">Http Client</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="isBackfilling">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterBackfill">Set to true if stream should stop after full load is finished</param>
    /// <param name="headerAuthenticatedMessageProvider">Authenticated message provider</param>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static RestApiSource Create(
        SimpleUriProvider uriProvider,
        FixedHeaderAuthenticatedMessageProvider headerAuthenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        HttpClient httpClient,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema)
    {
        return new RestApiSource(uriProvider, headerAuthenticatedMessageProvider, isBackfilling,
            changeCaptureInterval,
            lookBackInterval, httpClient, stopAfterBackfill, rateLimitPolicy, apiSchema);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="httpRequestTimeout">Http request timeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="isBackfilling">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterBackfill">Set to true if stream should stop after full load is finished</param>
    /// <param name="authHeaderAuthenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static RestApiSource Create(
        IPaginatedApiUriProvider uriProvider,
        DynamicBearerAuthenticatedMessageProvider authHeaderAuthenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        return new RestApiSource(uriProvider, authHeaderAuthenticatedMessageProvider, isBackfilling,
            changeCaptureInterval,
            lookBackInterval, httpRequestTimeout, stopAfterBackfill, rateLimitPolicy, apiSchema,
            responsePropertyKeyChain);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="httpRequestTimeout">Http request timeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="isBackfilling">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterBackfill">Set to true if stream should stop after full load is finished</param>
    /// <param name="headerAuthenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static RestApiSource Create(
        IPaginatedApiUriProvider uriProvider,
        FixedHeaderAuthenticatedMessageProvider headerAuthenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        TimeSpan httpRequestTimeout,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        return new RestApiSource(uriProvider, headerAuthenticatedMessageProvider, isBackfilling,
            changeCaptureInterval,
            lookBackInterval, httpRequestTimeout, stopAfterBackfill, rateLimitPolicy, apiSchema,
            responsePropertyKeyChain);
    }

    /// <summary>
    /// Creates new instance of <see cref="RestApiSource"/>
    /// </summary>
    /// <param name="uriProvider">URI provider</param>
    /// <param name="changeCaptureInterval">How often to track changes.</param>
    /// <param name="lookBackInterval">Look back interval</param>
    /// <param name="httpClient">Http request timeout</param>
    /// <param name="apiSchema">Api Schema</param>
    /// <param name="rateLimitPolicy">Rate limiting policy instance</param>
    /// <param name="isBackfilling">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterBackfill">Set to true if stream should stop after full load is finished</param>
    /// <param name="authHeaderAuthenticatedMessageProvider">Authenticated message provider</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    [ExcludeFromCodeCoverage(Justification = "Factory method")]
    public static RestApiSource Create(
        IPaginatedApiUriProvider uriProvider,
        DynamicBearerAuthenticatedMessageProvider authHeaderAuthenticatedMessageProvider,
        bool isBackfilling,
        TimeSpan changeCaptureInterval,
        TimeSpan lookBackInterval,
        HttpClient httpClient,
        bool stopAfterBackfill,
        AsyncRateLimitPolicy rateLimitPolicy,
        OpenApiSchema apiSchema,
        string[] responsePropertyKeyChain = null)
    {
        return new RestApiSource(uriProvider, authHeaderAuthenticatedMessageProvider, isBackfilling,
            changeCaptureInterval,
            lookBackInterval, httpClient, stopAfterBackfill, rateLimitPolicy, apiSchema,
            responsePropertyKeyChain);
    }

    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new SourceLogic(this);

    private sealed class SourceLogic : PollingSourceLogic, IStopAfterBackfill
    {
        private const string TimerKey = nameof(RestApiSource);
        private readonly LocalOnlyDecider decider;
        private readonly HttpClient httpClient;
        private readonly RestApiSource source;
        private HttpResponseMessage currentResponse;

        private Action<Task<Option<JsonElement>>> responseReceived;

        public SourceLogic(RestApiSource source) : base(source.changeCaptureInterval, source.Shape)
        {
            this.source = source;

            this.httpClient = this.source.httpClient ?? new HttpClient
            {
                Timeout = this.source.httpRequestTimeout
            };

            this.decider = Decider.From(ex => ex switch
            {
                IOException => Directive.Restart,
                TimeoutException => Directive.Restart,
                HttpRequestException => Directive.Restart,
                _ => Directive.Stop
            });


            this.SetHandler(source.Out, this.PullChanges, this.Finish);
        }

        /// <inheritdoc cref="IStopAfterBackfill.IsRunningInBackfillMode"/>
        public bool IsRunningInBackfillMode { get; set; }

        /// <inheritdoc cref="IStopAfterBackfill.StopAfterBackfill"/>
        public bool StopAfterBackfill => this.source.stopAfterBackfill;

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

            if (this.source.isBackfilling)
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
                    $"No data returned by latest call, next check in {this.ChangeCaptureInterval.TotalSeconds} seconds");

                this.ScheduleOnce(TimerKey, this.source.changeCaptureInterval);
            }
            else
            {
                this.EmitMultiple(this.source.Out, taskResult);
            }
        }

        private void PullChanges() =>
            this.source.rateLimitPolicy.ExecuteAsync(this.SendRequestOnce)
                .TryMap(result => result, HandleException)
                .ContinueWith(this.responseReceived);

        private Task<Option<JsonElement>> SendRequestOnce() => this.source
                .authenticatedMessageProvider
                .GetAuthenticatedMessage(this.httpClient)
                .Map(this.SendRequest)
                .Flatten();

        private Task<Option<JsonElement>> SendRequest(HttpRequestMessage msg)
        {
            this.Log.Debug("Successfully authenticated");
            var (maybeNextUri, requestMethod, maybePayload) = this.source.uriProvider.GetNextResultUri(
                this.currentResponse, this.IsRunningInBackfillMode, this.source.lookBackInterval,
                this.ChangeCaptureInterval);

            if (maybeNextUri.IsEmpty)
            {
                return Task.FromResult(Option<JsonElement>.None);
            }

            msg.RequestUri = maybeNextUri.Value;
            msg.Method = requestMethod;

            if (maybePayload.HasValue)
            {
                msg.Content = new StringContent(maybePayload.Value);
                if (!string.IsNullOrEmpty(maybePayload.Value))
                {
                    this.Log.Info($"Request payload for next result: {maybePayload.Value}");
                }
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
        }

        private static Option<JsonElement> HandleException(Exception exception) => exception switch
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
        };

        protected override void OnTimer(object timerKey)
        {
            this.PullChanges();
        }
    }
}
