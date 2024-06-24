using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Arcane.Framework.Sources.RestApi.Services.AuthenticatedMessageProviders.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Sources.RestApi.Services.AuthenticatedMessageProviders;

/// <summary>
/// Authenticated message provider that generated dynamic bearer token header.
/// </summary>
public record DynamicBearerAuthenticatedMessageProvider : IRestApiAuthenticatedMessageProvider
{
    private readonly TimeSpan expirationPeriod;
    private readonly string expirationPeriodPropertyName;
    private readonly HttpMethod requestMethod;
    private readonly string tokenPropertyName;
    private readonly string tokenRequestBody;
    private readonly string authHeaderName;
    private readonly string authScheme;
    private readonly Uri tokenSource;
    private readonly Dictionary<string, string> additionalHeaders;
    private string currentToken;
    private DateTimeOffset? validTo;

    /// <summary>
    /// Authenticated message provider that generated dynamic bearer token header.
    /// </summary>
    /// <param name="tokenSource">Token source address</param>
    /// <param name="tokenPropertyName">Token property name</param>
    /// <param name="expirationPeriodPropertyName">Token expiration property name</param>
    /// <param name="requestMethod">HTTP method for token request</param>
    /// <param name="tokenRequestBody">HTTP body for token request</param>
    /// <param name="authHeaderName">Authorization header name</param>
    /// <param name="authScheme">Authorization scheme</param>
    /// <param name="additionalHeaders">Additional token headers</param>
    public DynamicBearerAuthenticatedMessageProvider(string tokenSource,
        string tokenPropertyName,
        string expirationPeriodPropertyName,
        HttpMethod requestMethod = null,
        string tokenRequestBody = null,
        Dictionary<string, string> additionalHeaders = null,
        string authHeaderName = null,
        string authScheme = null)
    {
        this.tokenSource = new Uri(tokenSource);
        this.tokenPropertyName = tokenPropertyName;
        this.expirationPeriodPropertyName = expirationPeriodPropertyName;
        this.tokenRequestBody = tokenRequestBody;
        this.requestMethod = requestMethod ?? HttpMethod.Get;
        this.authHeaderName = authHeaderName;
        this.authScheme = authScheme;
        this.additionalHeaders = additionalHeaders ?? new Dictionary<string, string>();
    }

    /// <summary>
    /// Authenticated message provider that generated dynamic bearer token header.
    /// </summary>
    /// <param name="tokenSource">Token source address</param>
    /// <param name="tokenPropertyName">Token property name</param>
    /// <param name="expirationPeriod">Token expiration period</param>
    /// <param name="requestMethod">HTTP method for token request</param>
    /// <param name="tokenRequestBody">HTTP body for token request</param>
    /// <param name="additionalHeaders">Additional token headers</param>
    /// <param name="authHeaderName">Authorization header name</param>
    /// <param name="authScheme">Authorization scheme</param>
    public DynamicBearerAuthenticatedMessageProvider(string tokenSource,
        string tokenPropertyName,
        TimeSpan expirationPeriod,
        HttpMethod requestMethod = null,
        string tokenRequestBody = null,
        Dictionary<string, string> additionalHeaders = null,
        string authHeaderName = null,
        string authScheme = null)
    {
        this.tokenSource = new Uri(tokenSource);
        this.tokenPropertyName = tokenPropertyName;
        this.expirationPeriod = expirationPeriod;
        this.tokenRequestBody = tokenRequestBody;
        this.requestMethod = requestMethod ?? HttpMethod.Get;
        this.authHeaderName = authHeaderName;
        this.authScheme = authScheme;
        this.additionalHeaders = additionalHeaders ?? new Dictionary<string, string>();
    }

    /// <inheritdoc cref="IRestApiAuthenticatedMessageProvider.GetAuthenticatedMessage"/>
    public Task<HttpRequestMessage> GetAuthenticatedMessage(HttpClient httpClient)
    {

        if (this.validTo.GetValueOrDefault(DateTimeOffset.MaxValue) <  DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(1)))
        {
            return Task.FromResult(this.GetRequest());
        }

        var tokenHrm = new HttpRequestMessage(this.requestMethod, this.tokenSource);

        if (!string.IsNullOrEmpty(this.tokenRequestBody))
        {
            tokenHrm.Content = new StringContent(this.tokenRequestBody, Encoding.UTF8, "application/json");
        }

        return httpClient.SendAsync(tokenHrm, CancellationToken.None).Map(response =>
        {
            response.EnsureSuccessStatusCode();
            return response.Content.ReadAsStringAsync();
        }).FlatMap(result =>
        {
            var tokenResponse = JsonSerializer.Deserialize<JsonElement>(result);
            this.currentToken = tokenResponse.GetProperty(this.tokenPropertyName).GetString();
            this.validTo = !string.IsNullOrEmpty(this.expirationPeriodPropertyName)
                ? DateTimeOffset.UtcNow.AddSeconds(tokenResponse.GetProperty(this.expirationPeriodPropertyName)
                    .GetInt32())
                : DateTimeOffset.UtcNow.Add(this.expirationPeriod);
            return new HttpRequestMessage
            {
                Headers = { Authorization = new AuthenticationHeaderValue("Bearer", this.currentToken) }
            };
        });
    }

    private HttpRequestMessage GetRequest()
    {
        var request = new HttpRequestMessage();
        switch (this.authHeaderName)
        {
            case null or "" or "Authorization":
                request.Headers.Authorization = new AuthenticationHeaderValue(scheme: this.authScheme ?? "Bearer", this.currentToken);
                break;
            default:
                request.Headers.Add(this.authHeaderName,  string.IsNullOrEmpty(this.authScheme) ? this.currentToken : $"{this.authScheme} {this.currentToken}");
                break;
        }

        foreach (var (headerKey, headerValue) in this.additionalHeaders ?? new Dictionary<string, string>())
        {
            request.Headers.Add(headerKey, headerValue);
        }

        return request;
    }

}
