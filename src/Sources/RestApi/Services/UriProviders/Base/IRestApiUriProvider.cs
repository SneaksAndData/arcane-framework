using System;
using System.Net.Http;
using Akka.Util;

namespace Arcane.Framework.Sources.RestApi.Services.UriProviders.Base;

/// <summary>
/// Provides the next URI for the paginated REST API responses.
/// </summary>
public interface IRestApiUriProvider
{
    /// <summary>
    /// Base URI address.
    /// </summary>
    Uri BaseUri { get; }

    /// <summary>
    /// Returns URI for next page of the paginated response.
    /// </summary>
    /// <param name="paginatedResponse">Response body</param>
    /// <param name="isBackfill">True if the stream is running in the backfill mode</param>
    /// <param name="lookBackInterval">Interval in past to start read from</param>
    /// <param name="changeCaptureInterval">How often query REST API</param>
    /// <returns></returns>
    (Option<Uri> nextUri, HttpMethod requestMethod, Option<string> payload) GetNextResultUri(
        Option<HttpResponseMessage> paginatedResponse, bool isBackfill, TimeSpan lookBackInterval,
        TimeSpan changeCaptureInterval);
}
