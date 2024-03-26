using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Arcane.Framework.Sources.CdmChangeFeedSource.Models;

namespace Arcane.Framework.Sources.CdmChangeFeedSource.Extensions;

/// <summary>
/// Extenstion method for SimpleCdmAttribute class
/// </summary>
public static class SimpleCdmAttributeExtensions
{
    /// <summary>
    /// Resolve complex type to list of attributes
    /// </summary>
    public static IEnumerable<SimpleCdmAttribute> ResolveComplexTypes(this IEnumerable<SimpleCdmAttribute> attributes,
        IEnumerable<JsonElement> complexTypes)
    {
        return attributes.Select(attr =>
        {
            var complexTypesArray = complexTypes.ToArray();
            if (!string.IsNullOrEmpty(attr.DataFormat) && SimpleCdmAttribute.IsComplexType(attr.DataFormat))
            {
                attr.DataType = SimpleCdmAttribute.ResolveComplexType(
                    FindComplexTypeDefinition(complexTypesArray, attr.DataFormat), complexTypesArray);
                attr.DataFormat = null;
            }

            if (!string.IsNullOrEmpty(attr.DataType) && SimpleCdmAttribute.IsComplexType(attr.DataType))
            {
                attr.DataType =
                    SimpleCdmAttribute.ResolveComplexType(
                        FindComplexTypeDefinition(complexTypesArray, attr.DataType),
                        complexTypesArray);
                attr.DataFormat = null;

                return attr;
            }

            attr.DataType = attr.DataFormat ?? attr.DataType;
            attr.DataFormat = null;

            return attr;
        });
    }

    private static JsonElement FindComplexTypeDefinition(JsonElement[] complexTypes, string typeName)
    {
        var definitions = complexTypes
            .Where(ct => ct.GetProperty("dataTypeName").GetString() == typeName)
            .ToArray();
        if (!definitions.Any()) throw new InvalidOperationException($"Unknown primitive type: {typeName}");

        return definitions.FirstOrDefault();
    }
}
