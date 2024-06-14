using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.Extensions;
using Arcane.Framework.Sources.SalesForce.Models;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Sources.SalesForce.Services.AuthenticatedMessageProviders;

/// <summary>
/// Authenticated message provider that generated dynamic bearer token header.
/// </summary>
public record SalesForceJobProvider
{
    private readonly TimeSpan expirationPeriod;
    private string currentToken;
    private DateTimeOffset? validTo;
    private readonly Uri tokenSource;
    private readonly string accountName;
    private readonly string clientId;
    private readonly string clientSecret;
    private readonly string username;
    private readonly string password;
    private readonly string securityToken;
    private readonly string apiVersion;
    private Option<string> currentJobLocator;
    private readonly Option<int> rowsPerPage;

    /// <summary>
    /// Authenticated message provider that generated dynamic bearer token header.
    /// </summary>

    public SalesForceJobProvider(string accountName, string clientId, string clientSecret, string username, string password, string securityToken, string apiVersion, int rowsPerPage)
    {
        this.tokenSource = new Uri($"https://{accountName}/services/oauth2/token");
        this.accountName = accountName;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.securityToken = securityToken;
        this.apiVersion = apiVersion;
        this.currentJobLocator = Option<string>.None;
        this.rowsPerPage = rowsPerPage;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="accountName"></param>
    /// <param name="clientId"></param>
    /// <param name="clientSecret"></param>
    /// <param name="username"></param>
    /// <param name="password"></param>
    /// <param name="securityToken"></param>
    /// <param name="apiVersion"></param>
    public SalesForceJobProvider(string accountName, string clientId, string clientSecret, string username, string password, string securityToken, string apiVersion)
    {
        this.tokenSource = new Uri($"https://{accountName}/services/oauth2/token");
        this.accountName = accountName;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.securityToken = securityToken;
        this.apiVersion = apiVersion;
        this.currentJobLocator = Option<string>.None;
        this.rowsPerPage = Option<int>.None;
        this.expirationPeriod = TimeSpan.FromMinutes(19);
    }

    /// <summary>
    /// Generates authenticated message for the REST API request.
    /// </summary>
    /// <param name="httpClient">HTTP client</param>
    /// <returns>Authenticated message</returns>
    public Task<HttpRequestMessage> GetAuthenticatedMessage(HttpClient httpClient)
    {
        if (this.validTo.GetValueOrDefault(DateTimeOffset.MaxValue) <
            DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(1)))
        {
            return Task.FromResult(new HttpRequestMessage
            {
                Headers = { Authorization = new AuthenticationHeaderValue("Bearer", this.currentToken) }
            });
        }
        var dict = new Dictionary<string, string>
        {
            { "grant_type", "password" },
            { "client_id", this.clientId },
            { "client_secret", this.clientSecret },
            { "username", this.username },
            { "password", $"{this.password}{this.securityToken}" },
        };
        var tokenHrm = new HttpRequestMessage(HttpMethod.Post, this.tokenSource)
        {
            Content = new FormUrlEncodedContent(dict)
        };

        return httpClient.SendAsync(tokenHrm, CancellationToken.None).Map(response =>
        {
            response.EnsureSuccessStatusCode();
            return response.Content.ReadAsStringAsync();
        }).FlatMap(result =>
        {
            var tokenResponse = JsonSerializer.Deserialize<JsonElement>(result);
            this.currentToken = tokenResponse.GetProperty("access_token").GetString();
            this.validTo = DateTimeOffset.UtcNow.Add(this.expirationPeriod);
            return new HttpRequestMessage
            {
                Headers = { Authorization = new AuthenticationHeaderValue("Bearer", this.currentToken) }
            };
        });
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="httpClient"></param>
    /// <param name="entityName"></param>
    /// <returns></returns>
    public Task<Option<SalesForceEntity>> GetSchema(HttpClient httpClient, string entityName)
    {
        return this.GetAuthenticatedMessage(httpClient).Map(msg =>

            {
                msg.RequestUri = new Uri($"https://{this.accountName}/services/data/{this.apiVersion}/query?q=SELECT Name,DataType,ValueTypeId FROM EntityParticle WHERE EntityDefinition.QualifiedApiName ='{entityName}' and dataType != 'address'");

                return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content.ReadAsStringAsync().Map(value =>
                        {

                            return SalesForceEntity.FromJson(entityName, JsonSerializer.Deserialize<JsonDocument>(value)).AsOption();

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
                } => Option<SalesForceEntity>.None, // API rate limit, in case configured rate limit is not good enough
                HttpRequestException
                {
                    StatusCode: HttpStatusCode.RequestTimeout
                } => Option<SalesForceEntity>.None, // Potential server-side timeout due to overload
                _ => throw exception
            });
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="httpClient"></param>
    /// <param name="entitySchema"></param>
    /// <returns></returns>
    public Task<Option<SalesForceJob>> CreateJob(HttpClient httpClient, SalesForceEntity entitySchema)
    {
        return this.GetAuthenticatedMessage(httpClient).Map(msg =>

            {
                msg.RequestUri = new Uri($"https://{this.accountName}/services/data/{this.apiVersion}/jobs/query");
                msg.Content = JsonContent.Create(new
                {
                    operation = "query",
                    query = $"SELECT {entitySchema.Attributes.Where(e => e.DataType != "address").Select(e => e.Name).Aggregate((a, b) => a + ", " + b)} FROM {entitySchema.EntityName}",

                });
                msg.Method = HttpMethod.Post;

                return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content.ReadAsStringAsync().Map(value =>
                        {

                            return JsonSerializer.Deserialize<SalesForceJob>(value).AsOption();

                        });
                    }

                    var errorMsg = $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

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
            });
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="httpClient"></param>
    /// <param name="job"></param>
    /// <returns></returns>
    public Task<Option<SalesForceJob>> GetJobStatus(HttpClient httpClient, SalesForceJob job)
    {
        return this.GetAuthenticatedMessage(httpClient).Map(msg =>

            {
                msg.RequestUri = new Uri($"https://{this.accountName}/services/data/{this.apiVersion}/jobs/query/{job.Id}");

                return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content.ReadAsStringAsync().Map(value =>
                        {

                            return JsonSerializer.Deserialize<SalesForceJob>(value).AsOption();

                        });
                    }

                    var errorMsg = $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

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
            });
    }

    private (Type, object) ConvertToSalesforceType(string salesforceDataType, string value)
    {
        var tp = SalesForceAttribute.MapSalesforceType(salesforceDataType);
        var converter = TypeDescriptor.GetConverter(tp);
        return (tp, value == "" ? null : converter.ConvertFromInvariantString(value));
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="httpClient"></param>
    /// <param name="job"></param>
    /// <param name="entitySchema"></param>
    /// <returns></returns>
    public Task<IEnumerable<List<DataCell>>> GetJobResult(HttpClient httpClient, SalesForceJob job, SalesForceEntity entitySchema)
    {
        return this.GetAuthenticatedMessage(httpClient).Map(msg =>

            {
                var maxRowString = this.rowsPerPage.HasValue ? $"maxRecords={this.rowsPerPage.Value}" : "";
                var locatorString = this.currentJobLocator.HasValue ? $"locator={this.currentJobLocator.Value}" : "";
                var urlParams = string.Join("&", new[] { maxRowString, locatorString });
                msg.RequestUri = new Uri($"https://{this.accountName}/services/data/{this.apiVersion}/jobs/query/{job.Id}/results?{urlParams}");
                msg.Headers.Add("Accept", "text/csv");

                return httpClient.SendAsync(msg, default(CancellationToken)).Map(response =>
                {
                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content.ReadAsStringAsync().Map(value =>
                        {

                            this.currentJobLocator = response.Headers.GetValues("Sforce-Locator").Select(h => h == "null" ? Option<string>.None : h.AsOption()).First();
                            var rows = value.ReplaceQuotedNewlines().Split("\n").Skip(1).Where(line => !string.IsNullOrEmpty(line)).Select(line =>
                            {
                                var cells = line.ParseCsvLine(entitySchema.Attributes.Length).Select(
                                        (v, ix) =>
                                        {
                                            var (tp, value) = this.ConvertToSalesforceType(entitySchema.Attributes[ix].DataType, v);
                                            return new DataCell(entitySchema.Attributes[ix].Name, tp,
                                                value);
                                        }).ToList();
                                return cells;
                            });
                            return rows;


                        });
                    }

                    var errorMsg = $"API request to {msg.RequestUri} failed with {response.StatusCode}, reason: {response.ReasonPhrase}, content: {response.Content.ReadAsStringAsync().ConfigureAwait(false).GetAwaiter().GetResult()}";

                    throw new HttpRequestException(errorMsg, null, response.StatusCode);
                }).Flatten();
            }).Flatten();
    }
}
