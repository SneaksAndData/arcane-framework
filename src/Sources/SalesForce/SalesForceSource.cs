using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
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
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.Base;
using Arcane.Framework.Sources.Exceptions;
using Arcane.Framework.Sources.Extensions;
using Arcane.Framework.Sources.SalesForce.Models;
using Arcane.Framework.Sources.SalesForce.Services.AuthenticatedMessageProviders;
using Arcane.Framework.Sources.SalesForce.Services.AuthenticatedMessageProviders.Base;
using Arcane.Framework.Sources.SalesForce.Services.UriProviders;
using Arcane.Framework.Sources.SalesForce.Services.UriProviders.Base;
using Cassandra;
using Microsoft.IdentityModel.Tokens;
using Parquet.Data;
using Polly.RateLimit;
using Snd.Sdk.Tasks;
using ZstdNet;

namespace Arcane.Framework.Sources.SalesForce;

/// <summary>
/// Source for reading data from SalesForce BULK v2 API.
/// </summary>
public class SalesForceSource : GraphStage<SourceShape<List<DataCell>>>, IParquetSource, ITaggedSource
{
    private readonly string entityName;
    private readonly ISalesForceAuthenticatedMessageProvider _authenticatedMessageProvider;
    private readonly bool fullLoadOnStart;
    private readonly HttpClient httpClient;
    // private readonly AsyncRateLimitPolicy rateLimitPolicy;
    // private readonly bool stopAfterFullLoad;
    // private readonly ISalesForceUriProvider uriProvider;
    private readonly TimeSpan changeCaptureInterval;

    private SalesForceSource(

        ISalesForceAuthenticatedMessageProvider authenticatedMessageProvider,

        string entityName
        )
    {

        this._authenticatedMessageProvider = authenticatedMessageProvider;

        this.httpClient = new HttpClient();
        this.entityName = entityName;

        this.Shape = new SourceShape<List<DataCell>>(this.Out);
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
    /// <param name="fullLoadOnStart">Set to true to stream full current version of the table first.</param>
    /// <param name="stopAfterFullLoad">Set to true if stream should stop after full load is finished</param>
    private SalesForceSource(

        ISalesForceAuthenticatedMessageProvider authenticatedMessageProvider,
        string entityName,
        HttpClient httpClient,
        TimeSpan changeCaptureInterval) : this(authenticatedMessageProvider, entityName)
    {
        this.httpClient = httpClient;
    }


    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(SalesForceSource));

    /// <summary>
    /// Source outlet
    /// </summary>
    public Outlet<List<DataCell>> Out { get; } = new($"{nameof(SalesForceSource)}.Out");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SourceShape<List<DataCell>> Shape { get; }

    /// <inheritdoc cref="IParquetSource.GetParquetSchema"/>
    public Schema GetParquetSchema()
    {
        // TODO
        var response = this._authenticatedMessageProvider.GetAuthenticatedMessage(httpClient).Map(msg =>

           {
               msg.RequestUri = new Uri($"https://test.my.salesforce.com/services/data/v60.0/query?q=SELECT Name,DataType,ValueTypeId FROM EntityParticle WHERE EntityDefinition.QualifiedApiName ='{this.entityName}' and dataType != 'address'");

               return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
               {
                   if (response.IsSuccessStatusCode)
                   {
                       return response.Content.ReadAsStringAsync().Map(value =>
                       {

                           return JsonSerializer.Deserialize<JsonDocument>(value);

                       });
                   }

                   var errorMsg = $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

                   // this.Log.Warning(errorMsg);

                   throw new HttpRequestException(errorMsg, null, response.StatusCode);
               }).Flatten();
           }).Flatten().TryMap(result => result, exception => exception switch
           {
               HttpRequestException
               {
                   StatusCode: HttpStatusCode.TooManyRequests
               } => Option<JsonDocument>.None, // API rate limit, in case configured rate limit is not good enough
               HttpRequestException
               {
                   StatusCode: HttpStatusCode.RequestTimeout
               } => Option<JsonDocument>.None, // Potential server-side timeout due to overload
               _ => throw exception
           }).Result;



        if (response.HasValue)
        {
            return SalesForceEntity.FromJson(this.entityName, response.Value).GetReader().ToParquetSchema();

        }
        throw new Exception("Yikes");
        // return this.apiSchema.ToParquetSchema();
    }

    /// <inheritdoc cref="ITaggedSource.GetDefaultTags"/>
    public SourceTags GetDefaultTags()
    {
        return new SourceTags
        {
            SourceEntity = this.entityName,
        };
    }

    /// <summary>
    /// Creates new instance of <see cref="SalesForceSource"/>
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
    public static SalesForceSource Create(
        DynamicBearerAuthenticatedMessageProvider headerAuthenticatedMessageProvider,
        HttpClient httpClient,
        string entityName,
        TimeSpan changeCaptureInterval
     )
    {
        return new SalesForceSource(headerAuthenticatedMessageProvider
            , entityName, httpClient, changeCaptureInterval);
    }



    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new SourceLogic(this);
    }

    private sealed class SourceLogic : TimerGraphStageLogic
    {
        private const string TimerKey = nameof(SalesForceSource);
        private readonly LocalOnlyDecider decider;
        private readonly HttpClient httpClient;
        private readonly SalesForceSource source;
        private HttpResponseMessage currentResponse;
        private SalesForceEntity entitySchema;
        private Option<SalesForceJob> currentJob;
        private Option<string> jobLocator;


        // private SalesForceEntity salesForceEntity;
        private Action<Task<Option<JsonElement>>> responseReceived;

        public SourceLogic(SalesForceSource source) : base(source.Shape)
        {
            this.source = source;

            this.httpClient = this.source.httpClient ?? new HttpClient();

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

            this.currentJob = new SalesForceJob
            {
                Id = "",
                NumberRecordsProcessed = 0,
                Object = this.source.entityName,
                TotalProcessingTime = 0,
                Status = SalesforceJobStatus.None
            };

            this.jobLocator = Option<string>.None;

            this.SetHandler(source.Out, this.PullChanges, this.Finish);
        }

        /// <inheritdoc cref="IStopAfterBackfill.IsRunningInBackfillMode"/>
        public bool IsRunningInBackfillMode { get; set; }

        /// <inheritdoc cref="IStopAfterBackfill.StopAfterBackfill"/>

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
            this.Log.Info("Prestart");
            this.UpdateSchema();
            // base.PreStart();


            if (this.source.fullLoadOnStart)
            {
                this.IsRunningInBackfillMode = true;
            }
        }

        public void UpdateSchema()
        {
            var response = this.source._authenticatedMessageProvider.GetAuthenticatedMessage(httpClient).Map(msg =>

            {
                msg.RequestUri = new Uri($"https://test.my.salesforce.com/services/data/v60.0/query?q=SELECT Name,DataType,ValueTypeId FROM EntityParticle WHERE EntityDefinition.QualifiedApiName ='{this.source.entityName}' and dataType != 'address'");

                return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content.ReadAsStringAsync().Map(value =>
                        {
                            this.Log.Info(value);

                            return JsonSerializer.Deserialize<JsonDocument>(value);

                        });
                    }

                    var errorMsg = $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

                    // this.Log.Warning(errorMsg);

                    throw new HttpRequestException(errorMsg, null, response.StatusCode);
                }).Flatten();
            }).Flatten().TryMap(result => result, exception => exception switch
            {
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.TooManyRequests
                } => Option<JsonDocument>.None, // API rate limit, in case configured rate limit is not good enough
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.RequestTimeout
                } => Option<JsonDocument>.None, // Potential server-side timeout due to overload
                _ => throw exception
            }).Result;



            if (response.IsEmpty)
            {
                this.Log.Warning("Could not update schema");
            }
            else
            {
                var newSchema = SalesForceEntity.FromJson(this.source.entityName, response.Value);
                var schemaUnchanged =
                    SalesForceEntity.SalesForceEntityComparer.Equals(this.entitySchema, newSchema);
                this.entitySchema = (this.entitySchema == null, schemaEquals: schemaUnchanged) switch
                {
                    (true, _) or (false, true) => newSchema,
                    (false, false) => throw new SchemaMismatchException()
                };
            }

        }

        private void CreateNewJob()
        {
            // this.UpdateSchema();
            this.Log.Info("Creating new job");
            var response = this.source._authenticatedMessageProvider.GetAuthenticatedMessage(httpClient).Map(msg =>

            {
                msg.RequestUri = new Uri("https://test.my.salesforce.com/services/data/v60.0/jobs/query");
                msg.Content = JsonContent.Create(new
                {
                    operation = "query",
                    query = $"SELECT {this.entitySchema.Attributes.Where(e => e.DataType != "address").Select(e => e.Name).Aggregate((a, b) => a + ", " + b)} FROM {this.entitySchema.EntityName} limit 100",

                });
                msg.Method = HttpMethod.Post;

                return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content.ReadAsStringAsync().Map(value =>
                        {
                            this.Log.Info(value);
                            return JsonSerializer.Deserialize<SalesForceJob>(value);

                        });
                    }

                    var errorMsg = $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

                    // this.Log.Warning(errorMsg);

                    throw new HttpRequestException(errorMsg, null, response.StatusCode);
                }).Flatten();
            }).Flatten().TryMap(result => result, exception => exception switch
            {
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.TooManyRequests
                } => Option<SalesForceJob>.None, // API rate limit, in case configured rate limit is not good enough
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.RequestTimeout
                } => Option<SalesForceJob>.None, // Potential server-side timeout due to overload
                _ => throw exception
            }).Result;

            if (response.IsEmpty)
            {
                this.Log.Warning("Could not create job");
            }
            else
            {
                this.currentJob = response.Value;
            }
            this.ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
        }

        private void UpdateJobStatus()
        {
            this.Log.Info("Updating job status");
            var response = this.source._authenticatedMessageProvider.GetAuthenticatedMessage(httpClient).Map(msg =>

            {
                msg.RequestUri = new Uri($"https://test.my.salesforce.com/services/data/v60.0/jobs/query/{this.currentJob.Value.Id}");


                return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content.ReadAsStringAsync().Map(value =>
                        {
                            this.Log.Info(value);
                            return JsonSerializer.Deserialize<SalesForceJob>(value);

                        });
                    }

                    var errorMsg = $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

                    // this.Log.Warning(errorMsg);

                    throw new HttpRequestException(errorMsg, null, response.StatusCode);
                }).Flatten();
            }).Flatten().TryMap(result => result, exception => exception switch
            {
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.TooManyRequests
                } => Option<SalesForceJob>.None, // API rate limit, in case configured rate limit is not good enough
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.RequestTimeout
                } => Option<SalesForceJob>.None, // Potential server-side timeout due to overload
                _ => throw exception
            }).Result;

            if (response.IsEmpty)
            {
                this.Log.Warning("Could not create job");
            }
            else
            {
                this.currentJob = response.Value;
            }
            this.ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
        }


        private (Type, object) ConvertToSalesforceType(string salesforceDataType, string value)
        {
            var tp = SalesForceAttribute.MapSalesforceType(salesforceDataType);
            var converter = TypeDescriptor.GetConverter(tp);
            return (tp, value == "" ? null : converter.ConvertFromInvariantString(value));
        }


        private void ProcessResult()
        {
            this.Log.Info("Processing results. bip bop");
            var response = this.source._authenticatedMessageProvider.GetAuthenticatedMessage(httpClient).Map(msg =>
            {
                var locatorString = this.jobLocator.HasValue ? $"locator={this.jobLocator.Value}" : "";
                msg.RequestUri = new Uri($"https://test.my.salesforce.com/services/data/v60.0/jobs/query/{this.currentJob.Value.Id}/results?maxRecords=50&{locatorString}");
                msg.Headers.Add("Accept", "text/csv");
                return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content.ReadAsStringAsync().Map(value =>
                        {
                            this.Log.Info(value);
                            this.jobLocator = response.Headers.GetValues("Sforce-Locator").Select(v => v == "null" ? Option<string>.None : v.AsOption()).First();
                            return value;

                        });
                    }

                    var errorMsg = $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

                    // this.Log.Warning(errorMsg);

                    throw new HttpRequestException(errorMsg, null, response.StatusCode);
                }).Flatten();
            }).Flatten().Result;

            var rows = response.ReplaceQuotedNewlines().Split("\n").Skip(1).Where(line => !string.IsNullOrEmpty(line)).Select(line =>
            {
                var cells = line.ParseCsvLine(this.entitySchema.Attributes.Length).Select(
                        (v, ix) =>
                        {
                            var (tp, value) = this.ConvertToSalesforceType(this.entitySchema.Attributes[ix].DataType, v);
                            return new DataCell(this.entitySchema.Attributes[ix].Name, tp,
                                value);
                        }).ToList();
                return cells;
            });
            this.EmitMultiple(this.source.Out, rows);

            if (this.jobLocator.IsEmpty)
            {
                this.currentJob = Option<SalesForceJob>.None;
                this.ScheduleOnce(TimerKey, this.source.changeCaptureInterval);
            }

        }

        private void PullChanges()
        {
            switch (this.currentJob.Select(job => job.Status).GetOrElse(SalesforceJobStatus.None))
            {
                case SalesforceJobStatus.UploadComplete:; this.UpdateJobStatus(); break;
                case SalesforceJobStatus.InProgress: this.UpdateJobStatus(); break;
                case SalesforceJobStatus.Aborted: this.FailStage(new Exception("Something bad happened")); break;
                case SalesforceJobStatus.Failed: this.FailStage(new Exception("Something bad happened")); break;
                case SalesforceJobStatus.JobComplete: this.ProcessResult(); break;
                case SalesforceJobStatus.None: this.CreateNewJob(); break;
                default: this.FailStage(new Exception("Something bad happened")); break;
            };




        }

        protected override void OnTimer(object timerKey)
        {
            this.PullChanges();
        }
    }
}
