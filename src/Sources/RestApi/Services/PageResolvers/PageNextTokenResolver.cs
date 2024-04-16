using System.Linq;
using System.Net.Http;
using Akka.Util;
using Arcane.Framework.Sources.RestApi.Services.PageResolvers.Base;

namespace Arcane.Framework.Sources.RestApi.Services.PageResolvers;

/// <summary>
/// Page offset resolver for pages with next link
/// </summary>
public class PageNextTokenResolver : PageResolverBase<string>
{
    private readonly string[] nextPageTokenPropertyKeyChain;

    /// <summary>
    /// Page offset resolver for pages with next link
    /// </summary>
    /// <param name="nextPageTokenPropertyKeyChain">Optional property key chain for resolver property value like total pages or token value.</param>
    public PageNextTokenResolver(string[] nextPageTokenPropertyKeyChain)
    {
        this.nextPageTokenPropertyKeyChain = nextPageTokenPropertyKeyChain;
    }

    /// <inheritdoc cref="PageResolverBase{TPagePointer}.Next"/>
    public override bool Next(Option<HttpResponseMessage> apiResponse)
    {
        if (!apiResponse.IsEmpty)
        {
            // read next page token from response
            this.pagePointer = this.nextPageTokenPropertyKeyChain
                .Aggregate(this.GetResponse(apiResponse), (je, property) => je.GetProperty(property)).GetString();

            // check if we are starting to list pages, or are in the process already, or have finished
            return this.pagePointer switch
            {
                null => false,
                _ => true
            };
        }

        return string.IsNullOrEmpty(this.pagePointer);
    }
}
