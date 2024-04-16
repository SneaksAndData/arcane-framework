using System.Net.Http;
using System.Threading.Tasks;

namespace Arcane.Framework.Sources.RestApi.Services.AuthenticatedMessageProviders.Base;

/// <summary>
/// Authentication message provider interface for various REST API authentication methods.
/// </summary>
public interface IRestApiAuthenticatedMessageProvider
{
    /// <summary>
    /// Generates authenticated message for the REST API request.
    /// </summary>
    /// <param name="httpClient">HTTP client</param>
    /// <returns>Authenticated message</returns>
    Task<HttpRequestMessage> GetAuthenticatedMessage(HttpClient httpClient);
}
