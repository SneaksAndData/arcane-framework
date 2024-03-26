using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Arcane.Framework.Sources.RestApi.Services.AuthenticatedMessageProviders.Base;

namespace Arcane.Framework.Sources.RestApi.Services.AuthenticatedMessageProviders;

/// <summary>
/// Authenticated message provider that adds fixed headers to the request.
/// </summary>
public record FixedHeaderAuthenticatedMessageProvider : IRestApiAuthenticatedMessageProvider
{
    private readonly Dictionary<string, string> customHeaders;

    /// <summary>
    /// Authenticated message provider that adds fixed headers to the request.
    /// </summary>
    /// <param name="customHeaders">Headers collection to add to the HTTP message</param>
    public FixedHeaderAuthenticatedMessageProvider(Dictionary<string, string> customHeaders)
    {
        this.customHeaders = customHeaders;
    }

    /// <inheritdoc cref="IRestApiAuthenticatedMessageProvider.GetAuthenticatedMessage"/>
    public Task<HttpRequestMessage> GetAuthenticatedMessage(HttpClient httpClient = null)
    {
        var msg = new HttpRequestMessage();
        foreach (var (header, headerValue) in customHeaders) msg.Headers.Add(header, headerValue);

        return Task.FromResult(msg);
    }
}
