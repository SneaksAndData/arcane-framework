using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Arcane.Framework.Sources.SalesForce.Models;

/// <summary>
/// Represents Salesforce attribute
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
    /// Attribute data type
    /// </summary>
    [JsonPropertyName("ValueTypeId")]
    public string DataType { get; set; }


    /// <summary>
    /// Attribute comparer
    /// </summary>
    public static IEqualityComparer<SalesForceAttribute> SalesForceAttributeComparer { get; } =
        new SalesForceAttributeEqualityComparer();

    /// <summary>
    /// // Maps Salesforce type to .NET type
    /// </summary>
    /// <param name="salesforceTypeName">Salesforce type name</param>
    /// <returns>.NET type instance</returns>
    /// <exception cref="InvalidOperationException">Thrown if type is not supported</exception>
    public static Type MapSalesforceType(string salesforceTypeName)
    {
        if (salesforceTypeMap.ContainsKey(salesforceTypeName.ToLower()))
        {
            return salesforceTypeMap[salesforceTypeName.ToLower()];
        }

        throw new InvalidOperationException($"Unsupported type: {salesforceTypeName}");
    }

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
