using System.Linq;
using System.Net.Http;
using Akka.Util;
using Arcane.Framework.Sources.RestApi.Services.PageResolvers.Base;

namespace Arcane.Framework.Sources.RestApi.Services.PageResolvers;

/// <summary>
/// Page offset resolver for numeric page pointers.
/// </summary>
public sealed class PageCountingResolver : PageResolverBase<int?>
{
    private readonly string[] totalPagesPropertyKeyChain;
    private int? totalPages;

    /// <summary>
    /// Page offset resolver for numeric page pointers.
    /// </summary>
    /// <param name="totalPagesPropertyKeyChain">Optional property key chain for resolver property value like total pages or token value.</param>
    public PageCountingResolver(string[] totalPagesPropertyKeyChain)
    {
        this.totalPagesPropertyKeyChain = totalPagesPropertyKeyChain;
        totalPages = null;
    }


    /// <inheritdoc cref="PageResolverBase{TPagePointer}.Next"/>
    public override bool Next(Option<HttpResponseMessage> apiResponse)
    {
        if (!apiResponse.IsEmpty)
        {
            // read total pages from the first response
            totalPages ??= totalPagesPropertyKeyChain
                .Aggregate(GetResponse(apiResponse), (je, property) => je.GetProperty(property)).GetInt32();

            // check if we are starting to list pages, or are in the process already, or have finished
            switch (pagePointer)
            {
                case null:
                    pagePointer = 1;
                    return true;
                case var value when value < totalPages:
                    pagePointer += 1;
                    return true;
                default:
                    pagePointer = null;
                    totalPages = null;
                    return false;
            }
        }

        if (!pagePointer.HasValue)
        {
            pagePointer = 1;
            return true;
        }

        // next got called without overfilling the total pages, but empty response was received - maybe page count was incorrect - thus do a full reset
        pagePointer = null;
        totalPages = null;
        return false;
    }
}
