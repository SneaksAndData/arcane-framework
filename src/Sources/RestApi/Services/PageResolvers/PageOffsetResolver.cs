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
    private readonly int? startOffset;

    /// <summary>
    /// Page offset resolver for numeric page pointers.
    /// </summary>
    /// <param name="responseSize">Total pages in response</param>
    /// <param name="responseBodyPropertyKeyChain">Optional property key chain for resolver property value like total pages or token value.</param>
    /// <param name="startOffset">First page offset</param>
    public PageOffsetResolver(int responseSize, string[] responseBodyPropertyKeyChain, int? startOffset)
    {
        this.responseSize = responseSize;
        this.responseBodyPropertyKeyChain = responseBodyPropertyKeyChain;
        this.startOffset = startOffset;
    }

    /// <inheritdoc cref="PageResolverBase{TPagePointer}.Next"/>
    public override bool Next(Option<HttpResponseMessage> apiResponse)
    {
        if (apiResponse.HasValue)
        {
            if (!this.GetResponseContent(apiResponse, this.responseBodyPropertyKeyChain).Any())
            {
                if (this.pagePointer == null)
                {
                    this.pagePointer = this.startOffset ?? 0;
                    return true;
                }

                this.pagePointer = null;
                return false;
            }

            this.pagePointer += this.responseSize;
            return true;
        }

        if (this.pagePointer.HasValue)
        {
            this.pagePointer = null;
            return false;
        }

        this.pagePointer = this.startOffset ?? 0;
        return true;
    }
}
