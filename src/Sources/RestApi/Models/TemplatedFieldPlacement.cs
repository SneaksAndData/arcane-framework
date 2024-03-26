namespace Arcane.Framework.Sources.RestApi.Models;

/// <summary>
/// Placement of a templated field in a REST API request or request body.
/// </summary>
public enum TemplatedFieldPlacement
{
    /// <summary>
    /// Templated field is placed in the URL.
    /// </summary>
    URL,

    /// <summary>
    /// Templated field is placed in the request body.
    /// </summary>
    BODY
}
