using System;
using System.Linq;
using System.Text;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Models;
using Microsoft.OpenApi.Readers;

namespace Arcane.Framework.Sources.RestApi.Extensions;

/// <summary>
/// Extension methods for string operations related to REST API Source.
/// </summary>
public static class StringExtensions
{
    /// <summary>
    /// Parse OpenApi schema from base64 encoded string.
    /// </summary>
    /// <param name="schemaEncoded">Base64 encoded OpenApi schema</param>
    /// <returns>OpenApi schema</returns>
    public static OpenApiSchema ParseOpenApiSchema(this string schemaEncoded)
    {
        var openApiStringReader = new OpenApiStringReader();
        var schemaValue = Encoding.UTF8.GetString(Convert.FromBase64String(schemaEncoded));
        var openApiSchema = openApiStringReader.ReadFragment<OpenApiSchema>(schemaValue, OpenApiSpecVersion.OpenApi3_0,
                out var diagnostic);
        if (diagnostic.Errors.Count > 0)
        {
            throw new ApplicationException(
                $"Encountered errors when parsing OpenApi V3 schema: {schemaValue}, error: {string.Join("\n", diagnostic.Errors.Select(err => err.Message))}");
        }

        return openApiSchema;
    }
}
