using System.Text.Json.Serialization;

namespace Arcane.Framework.Sources.RestApi.Models;

/// <summary>
/// Page resolver configuration for the uri provider.
/// </summary>
public sealed class PageResolverConfiguration
{
    /// <summary>
    /// Type of the page resolver.
    /// </summary>
    [JsonPropertyName("resolverType")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public PageResolverType ResolverType { get; init; }

    /// <summary>
    /// Optional property key chain for resolver property value like total pages or token value.
    /// </summary>
    [JsonPropertyName("resolverPropertyKeyChain")]
    public string[] ResolverPropertyKeyChain { get; init; }

    /// <summary>
    /// Optional expected response size in number elements.
    /// </summary>
    [JsonPropertyName("responseSize")]
    public int? ResponseSize { get; init; }

    /// <summary>
    /// Optional start offset of the first page
    /// </summary>
    [JsonPropertyName("startOffset")]
    public int? StartOffset { get; init; }
}
