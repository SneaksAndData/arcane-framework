using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Arcane.Framework.Sources.SalesForce.Services.AuthenticatedMessageProviders.Base;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Sources.SalesForce.Services.AuthenticatedMessageProviders;

/// <summary>
/// Authenticated message provider that generated dynamic bearer token header.
/// </summary>
public record DynamicBearerAuthenticatedMessageProvider : ISalesForceAuthenticatedMessageProvider
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

    /// <summary>
    /// Authenticated message provider that generated dynamic bearer token header.
    /// </summary>

    public DynamicBearerAuthenticatedMessageProvider(string accountName, string clientId, string clientSecret, string username, string password, string securityToken)
    {
        this.tokenSource = new Uri($"https://{accountName}/services/oauth2/token");
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.securityToken = securityToken;
    }

    /// <inheritdoc cref="ISalesForceAuthenticatedMessageProvider.GetAuthenticatedMessage"/>
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
}
