using System.Linq;
using System.Net.Http;
using Akka.Util;
using Arcane.Framework.Sources.RestApi.Services.PageResolvers.Base;

namespace Arcane.Framework.Sources.RestApi.Services.PageResolvers;

/// <summary>
/// Page offset resolver for numeric page pointers.
/// </summary>
public sealed class PageOffsetResolver : PageResolverBase<int?>
{
    private readonly string[] responseBodyPropertyKeyChain;
    private readonly int responseSize;

    /// <summary>
    /// Page offset resolver for numeric page pointers.
    /// </summary>
    /// <param name="responseSize">Total pages in response</param>
    /// <param name="responseBodyPropertyKeyChain">Optional property key chain for resolver property value like total pages or token value.</param>
    public PageOffsetResolver(int responseSize, string[] responseBodyPropertyKeyChain)
    {
        this.responseSize = responseSize;
        this.responseBodyPropertyKeyChain = responseBodyPropertyKeyChain;
    }

    /// <inheritdoc cref="PageResolverBase{TPagePointer}.Next"/>
    public override bool Next(Option<HttpResponseMessage> apiResponse)
    {
        if (apiResponse.HasValue)
        {
            if (!GetResponseContent(apiResponse, responseBodyPropertyKeyChain).Any())
            {
                pagePointer = null;
                return false;
            }

            pagePointer += responseSize;
            return true;
        }

        if (pagePointer.HasValue)
        {
            pagePointer = null;
            return false;
        }

        pagePointer = 0;
        return true;
    }
}
