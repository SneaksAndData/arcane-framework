using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Models;
using Microsoft.OpenApi.Readers;

namespace Arcane.Framework.Sources.RestApi.Extensions;

/// <summary>
/// Contains extension methods for REST API operations.
/// </summary>
public static class RestApiExtensions
{
    /// <summary>
    /// Parse OpenApi schema from base64 encoded string.
    /// </summary>
    /// <param name="schemaEncoded">Base64 encoded OpenApi schema</param>
    /// <returns>OpenApi schema</returns>
    public static OpenApiSchema ParseOpenApiSchema(string schemaEncoded)
    {
        var openApiStringReader = new OpenApiStringReader();
        var schemaValue = Encoding.UTF8.GetString(Convert.FromBase64String(schemaEncoded));
        var openApiSchema =
            openApiStringReader.ReadFragment<OpenApiSchema>(schemaValue, OpenApiSpecVersion.OpenApi3_0,
                out var diagnostic);
        if (diagnostic.Errors.Count > 0)
        {
            throw new ApplicationException(
                $"Encountered errors when parsing OpenApi V3 schema: {schemaValue}, error: {string.Join("\n", diagnostic.Errors.Select(err => err.Message))}");
        }

        return openApiSchema;
    }

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
