namespace Arcane.Framework.Sources.RestApi.Models;

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
    /// Templated field is a date diapason start.
    /// </summary>
    FILTER_DATE_BETWEEN_FROM,

    /// <summary>
    /// Templated field is a date diapason end.
    /// </summary>
    FILTER_DATE_BETWEEN_TO
}
