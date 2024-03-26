using System.Net.Http;
using Akka.Util;
using Arcane.Framework.Sources.RestApi.Models;

namespace Arcane.Framework.Sources.RestApi.Services.PageResolvers.Base;

/// <summary>
/// Next page resolver adapter for different paginated REST API responses.
/// </summary>
public interface IPageResolver
{
    /// <summary>
    /// Moves page pointer to a next page. If there are no pages left, returns false, otherwise true.
    /// </summary>
    /// <param name="apiResponse"></param>
    /// <returns></returns>
    bool Next(Option<HttpResponseMessage> apiResponse);

    /// <summary>
    /// Resolves the template using the provided template field and the page point.
    /// </summary>
    /// <param name="template">Api body or url template.</param>
    /// <param name="pageField">Template field that contains a page pointer value.</param>
    /// <returns></returns>
    RestApiTemplate ResolvePage(RestApiTemplate template, RestApiTemplatedField pageField);
}
