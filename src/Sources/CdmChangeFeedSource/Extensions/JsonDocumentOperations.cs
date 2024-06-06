using System.Linq;
using System.Text.Json;

namespace Arcane.Framework.Sources.CdmChangeFeedSource.Extensions;

/// <summary>
/// Contains operations for parsing JSON documents.
/// </summary>
internal static class JsonDocumentOperations
{
    /// <summary>
    /// Get an element from a JSON array
    /// </summary>
    /// <param name="document">JSON element</param>
    /// <param name="searchFrom">Search from</param>
    /// <param name="searchFor">Element to search for</param>
    public static JsonElement GetArrayElement(this JsonElement document, string searchFrom, string searchFor)
    {
        return document
            .GetProperty(searchFrom)
            .EnumerateArray()
            .ToArray()
            .FirstOrDefault(prop => prop.TryGetProperty(searchFor, out _)).GetProperty(searchFor);
    }

    /// <summary>
    /// Get an element from a JSON array
    /// </summary>
    /// <param name="document">JSON element</param>
    /// <param name="searchFrom">Search from</param>
    /// <param name="searchFor">Element to search for</param>
    /// <param name="searchForValue">Value to search for</param>
    public static JsonElement GetArrayElement(this JsonElement document, string searchFrom, string searchFor,
        string searchForValue)
    {
        return document
            .GetProperty(searchFrom)
            .EnumerateArray()
            .ToArray().FirstOrDefault(prop => prop.GetProperty(searchFor).GetString() == searchForValue);
    }

    /// <summary>
    /// Filter an array of JSON elements
    /// </summary>
    /// <param name="document">JSON element</param>
    /// <param name="arrayProperty">Property to filter on</param>
    /// <param name="filterValue">Value to filter on</param>
    public static JsonElement FilterArray(this JsonElement document, string arrayProperty, string filterValue)
    {
        return document.EnumerateArray()
            .ToArray()
            .FirstOrDefault(trait => trait.GetProperty(arrayProperty).GetString() == filterValue);
    }
}
