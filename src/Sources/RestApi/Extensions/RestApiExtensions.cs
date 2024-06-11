using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace Arcane.Framework.Sources.RestApi.Extensions;

/// <summary>
/// Contains extension methods for REST API operations.
/// </summary>
internal static class RestApiExtensions
{

    /// <summary>
    /// Try to enumerate JSON array.
    /// </summary>
    /// <param name="jsonElement">JSON element</param>
    /// <returns>Array enumerator</returns>
    private static JsonElement.ArrayEnumerator TryEnumerateArray(this JsonElement jsonElement)
    {
        try
        {
            return jsonElement.EnumerateArray();
        }
        catch
        {
            return new JsonElement.ArrayEnumerator();
        }
    }

    /// <summary>
    /// Parse JSON response.
    /// </summary>
    /// <param name="response">JSON response</param>
    /// <param name="responsePropertyKeyChain">Response property key chain</param>
    /// <returns>JSON elements</returns>
    public static IEnumerable<JsonElement> ParseResponse(this JsonElement response, string[] responsePropertyKeyChain)
    {
        responsePropertyKeyChain ??= Array.Empty<string>();
        return !responsePropertyKeyChain.Any()
            ? response.EnumerateArray().AsEnumerable()
            : responsePropertyKeyChain.Aggregate(response,
                (je, property) =>
                {
                    if (je.TryGetProperty(property, out var propertyValue))
                    {
                        return propertyValue;
                    }

                    return JsonSerializer.Deserialize<JsonElement>($"{{ \"{property}\": {{ }}  }}");
                }).TryEnumerateArray().AsEnumerable();
    }
}
