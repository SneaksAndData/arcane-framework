namespace Arcane.Framework.Sources.RestApi.Services.UriProviders.Base;

/// <summary>
/// Provides URI for paginated API sources
/// </summary>
public interface IPaginatedApiUriProvider : IRestApiUriProvider
{
    /// <summary>
    /// Returns true if all pages have been read
    /// </summary>
    bool HasReadAllPages();
}
