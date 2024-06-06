namespace Arcane.Framework.Sources.RestApi.Models;

/// <summary>
/// Type of the templated field in a REST API request or request body.
/// </summary>
public enum TemplatedFieldType
{
    /// <summary>
    /// Templated field is a request page.
    /// </summary>
    RESPONSE_PAGE,

    /// <summary>
    /// Templated field is a start date filter.
    /// </summary>
    FILTER_DATE_FROM,

    /// <summary>
    /// Templated field is a date range start.
    /// </summary>
    FILTER_DATE_BETWEEN_FROM,

    /// <summary>
    /// Templated field is a date range end.
    /// </summary>
    FILTER_DATE_BETWEEN_TO
}
