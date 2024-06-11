using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using Akka.Util;
using Arcane.Framework.Sources.RestApi.Extensions;
using Arcane.Framework.Sources.RestApi.Models;

namespace Arcane.Framework.Sources.RestApi.Services.PageResolvers.Base;

/// <summary>
/// Base class for page resolvers.
/// </summary>
/// <typeparam name="TPagePointer">Type of a page pointer</typeparam>
internal abstract class PageResolverBase<TPagePointer> : IPageResolver
{
    protected TPagePointer pagePointer;

    /// <summary>
    /// Base class constructor.
    /// </summary>
    protected PageResolverBase()
    {
        this.pagePointer = default;
    }

    /// <summary>
    /// Advance the page pointer and return true if there are more pages to be read, or false if paging has reached the end.
    /// </summary>
    /// <returns></returns>
    public abstract bool Next(Option<HttpResponseMessage> apiResponse);

    /// <inheritdoc cref="IPageResolver.ResolvePage"/>
    public RestApiTemplate ResolvePage(RestApiTemplate template, RestApiTemplatedField pageField)
    {
        return template.ResolveField(pageField.FieldName, this.pagePointer.ToString());
    }

    /// <summary>
    /// Deserialize the response content into a JsonElement.
    /// </summary>
    /// <param name="apiResponse">Api response message (if any)</param>
    /// <returns>Deserialized API response</returns>
    protected JsonElement GetResponse(Option<HttpResponseMessage> apiResponse)
    {
        return JsonSerializer.Deserialize<JsonElement>(apiResponse.Value.Content
            .ReadAsStringAsync()
            .ConfigureAwait(false)
            .GetAwaiter()
            .GetResult());
    }

    /// <summary>
    /// Read response content properties and parse it into a JsonElements.
    /// </summary>
    /// <param name="apiResponse">API response</param>
    /// <param name="responseBodyPropertyKeyChain">List of keys to extract from API response</param>
    /// <returns>Deserialized API response</returns>
    protected IEnumerable<JsonElement> GetResponseContent(Option<HttpResponseMessage> apiResponse,
        string[] responseBodyPropertyKeyChain)
    {
        return this.GetResponse(apiResponse).ParseResponse(responseBodyPropertyKeyChain);
    }
}
