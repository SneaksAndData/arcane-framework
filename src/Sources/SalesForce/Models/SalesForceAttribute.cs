using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Arcane.Framework.Sources.SalesForce.Models;

/// <summary>
/// Represents CDM Change Feed attribute
/// </summary>
public class SalesForceAttribute
{
    private static readonly Dictionary<string, Type> salesforceTypeMap = new()
    {
        { "string", typeof(string) },
        { "id", typeof(string) },
        { "address", typeof(string) },
        { "datetime", typeof(DateTime) },
        { "date", typeof(DateTime) },
        { "decimal", typeof(decimal) },
        { "integer", typeof(int) },
        { "long", typeof(long) },
        { "double", typeof(double) },
        { "boolean", typeof(bool) },
    };

    /// <summary>
    /// Attribute name
    /// </summary>
    [JsonPropertyName("Name")]
    public string Name { get; set; }

    /// <summary>
    /// Attribute data format
    /// </summary>
    [JsonPropertyName("ValueTypeId")]
    public string DataType { get; set; }


    /// <summary>
    /// Attribute comparer
    /// </summary>
    public static IEqualityComparer<SalesForceAttribute> SalesForceAttributeComparer { get; } =
        new SalesForceAttributeEqualityComparer();

    /// <summary>
    /// Returns true if the type is composition of other types
    /// </summary>
    /// <param name="cdmTypeName"></param>
    /// <returns></returns>
    // public static bool IsComplexType(string cdmTypeName)
    // {
    //     return !cdmTypeMap.ContainsKey(cdmTypeName.ToLower());
    // }

    // /// <summary>
    // /// // Maps CDM type to .NET type
    // /// </summary>
    // /// <param name="cdmTypeName">CDM type name</param>
    // /// <returns>.NET type instance</returns>
    // /// <exception cref="InvalidOperationException">Thrown if type is not supported</exception>
    public static Type MapSalesforceType(string salesforceTypeName)
    {
        if (salesforceTypeMap.ContainsKey(salesforceTypeName.ToLower()))
        {
            return salesforceTypeMap[salesforceTypeName.ToLower()];
        }

        throw new InvalidOperationException($"Unsupported type: {salesforceTypeName}");
    }

    // /// <summary>
    // /// Resolves complex type
    // /// </summary>
    // /// <param name="startFrom">Start element for type resolution</param>
    // /// <param name="types">Element types list</param>
    // /// <returns></returns>
    // public static string ResolveComplexType(JsonElement startFrom, IEnumerable<JsonElement> types)
    // {
    //     var objectKind = startFrom.GetProperty("extendsDataType").ValueKind;
    //     var typeName = objectKind == JsonValueKind.Object
    //         ? startFrom.GetProperty("extendsDataType").GetProperty("dataTypeReference").GetString()
    //         : startFrom.GetProperty("extendsDataType").GetString();
    //     if (!IsComplexType(typeName))
    //     {
    //         return typeName;
    //     }

    //     return ResolveComplexType(
    //         types.Where(tp => tp.GetProperty("dataTypeName").GetString() == typeName).FirstOrDefault(), types);
    // }

    private sealed class SalesForceAttributeEqualityComparer : IEqualityComparer<SalesForceAttribute>
    {
        public bool Equals(SalesForceAttribute x, SalesForceAttribute y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }

            if (ReferenceEquals(x, null))
            {
                return false;
            }

            if (ReferenceEquals(y, null))
            {
                return false;
            }

            if (x.GetType() != y.GetType())
            {
                return false;
            }

            return x.Name == y.Name &&
                   x.DataType == y.DataType;
        }

        public int GetHashCode(SalesForceAttribute obj)
        {
            return HashCode.Combine(obj.Name, obj.DataType);
        }
    }
}
