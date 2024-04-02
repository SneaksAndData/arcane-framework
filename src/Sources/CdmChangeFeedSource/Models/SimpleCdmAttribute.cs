using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Arcane.Framework.Sources.CdmChangeFeedSource.Models;

/// <summary>
/// Represents CDM Change Feed attribute
/// </summary>
public class SimpleCdmAttribute
{
    private static readonly Dictionary<string, Type> cdmTypeMap = new()
    {
        { "string", typeof(string) },
        { "datetime", typeof(DateTime) },
        { "date", typeof(DateTime) },
        { "time", typeof(int) },
        { "int64", typeof(long) },
        { "int32", typeof(int) },
        { "integer", typeof(int) },
        { "decimal", typeof(decimal) },
        { "biginteger", typeof(long) },
        { "listlookupwellknown", typeof(string) },
        { "noyes", typeof(int) },
        { "guid", typeof(string) },
        { "binary", typeof(string) }
    };

    /// <summary>
    /// Attribute name
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; }

    /// <summary>
    /// Attribute data format
    /// </summary>
    [JsonPropertyName("dataFormat")]
    public string DataFormat { get; set; }

    /// <summary>
    /// Attribute data type
    /// </summary>
    [JsonPropertyName("dataType")]
    public string DataType { get; set; }

    /// <summary>
    /// Attribute description
    /// </summary>
    [JsonPropertyName("description")]
    public string Description { get; set; }

    /// <summary>
    /// True if attribute is nullable
    /// </summary>
    [JsonPropertyName("isNullable")]
    public bool? Nullable { get; set; }

    /// <summary>
    /// Attribute display name
    /// </summary>
    [JsonPropertyName("displayName")]
    public string DisplayName { get; set; }

    /// <summary>
    /// Attribute comparer
    /// </summary>
    public static IEqualityComparer<SimpleCdmAttribute> SimpleCdmAttributeComparer { get; } =
        new SimpleCdmAttributeEqualityComparer();

    /// <summary>
    /// Returns true if the type is composition of other types
    /// </summary>
    /// <param name="cdmTypeName"></param>
    /// <returns></returns>
    public static bool IsComplexType(string cdmTypeName)
    {
        return !cdmTypeMap.ContainsKey(cdmTypeName.ToLower());
    }

    /// <summary>
    /// // Maps CDM type to .NET type
    /// </summary>
    /// <param name="cdmTypeName">CDM type name</param>
    /// <returns>.NET type instance</returns>
    /// <exception cref="InvalidOperationException">Thrown if type is not supported</exception>
    public static Type MapCdmType(string cdmTypeName)
    {
        if (cdmTypeMap.ContainsKey(cdmTypeName.ToLower()))
        {
            return cdmTypeMap[cdmTypeName.ToLower()];
        }

        throw new InvalidOperationException($"Unsupported type: {cdmTypeName}");
    }

    /// <summary>
    /// Resolves complex type
    /// </summary>
    /// <param name="startFrom">Start element for type resolution</param>
    /// <param name="types">Element types list</param>
    /// <returns></returns>
    public static string ResolveComplexType(JsonElement startFrom, IEnumerable<JsonElement> types)
    {
        var objectKind = startFrom.GetProperty("extendsDataType").ValueKind;
        var typeName = objectKind == JsonValueKind.Object
            ? startFrom.GetProperty("extendsDataType").GetProperty("dataTypeReference").GetString()
            : startFrom.GetProperty("extendsDataType").GetString();
        if (!IsComplexType(typeName))
        {
            return typeName;
        }

        return ResolveComplexType(
            types.Where(tp => tp.GetProperty("dataTypeName").GetString() == typeName).FirstOrDefault(), types);
    }

    private sealed class SimpleCdmAttributeEqualityComparer : IEqualityComparer<SimpleCdmAttribute>
    {
        public bool Equals(SimpleCdmAttribute x, SimpleCdmAttribute y)
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
                   x.DataFormat == y.DataFormat &&
                   x.DataType == y.DataType &&
                   x.Description == y.Description &&
                   x.Nullable == y.Nullable &&
                   x.DisplayName == y.DisplayName;
        }

        public int GetHashCode(SimpleCdmAttribute obj)
        {
            return HashCode.Combine(obj.Name, obj.DataFormat, obj.DataType, obj.Description, obj.Nullable,
                obj.DisplayName);
        }
    }
}
