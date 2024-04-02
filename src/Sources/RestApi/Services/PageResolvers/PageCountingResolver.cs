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
        this.totalPages = null;
    }


    /// <inheritdoc cref="PageResolverBase{TPagePointer}.Next"/>
    public override bool Next(Option<HttpResponseMessage> apiResponse)
    {
        if (!apiResponse.IsEmpty)
        {
            // read total pages from the first response
            this.totalPages ??= this.totalPagesPropertyKeyChain
                .Aggregate(GetResponse(apiResponse), (je, property) => je.GetProperty(property)).GetInt32();

            // check if we are starting to list pages, or are in the process already, or have finished
            switch (this.pagePointer)
            {
                case null:
                    this.pagePointer = 1;
                    return true;
                case var value when value < this.totalPages:
                    this.pagePointer += 1;
                    return true;
                default:
                    this.pagePointer = null;
                    this.totalPages = null;
                    return false;
            }
        }

        if (!this.pagePointer.HasValue)
        {
            this.pagePointer = 1;
            return true;
        }

        // next got called without overfilling the total pages, but empty response was received - maybe page count was incorrect - thus do a full reset
        this.pagePointer = null;
        this.totalPages = null;
        return false;
    }
}
