using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using Arcane.Framework.Sources.CdmChangeFeedSource.Extensions;

namespace Arcane.Framework.Sources.CdmChangeFeedSource.Models;

/// <summary>
/// Represents CDM Change Feed entity
/// </summary>
public class SimpleCdmEntity
{
    /// <summary>
    /// Entity name
    /// </summary>
    public string EntityName { get; set; }

    /// <summary>
    /// Entity Version number
    /// </summary>
    public string VersionNumber { get; set; }

    /// <summary>
    /// Attributes collection
    /// </summary>
    public SimpleCdmAttribute[] Attributes { get; set; }

    /// <summary>
    /// Comparer class
    /// </summary>
    public static IEqualityComparer<SimpleCdmEntity> SimpleCdmEntityComparer { get; } =
        new SimpleCdmEntityEqualityComparer();

    /// <summary>
    /// Parse CDM entity from a JSON document
    /// </summary>
    /// <param name="document">Json document to parse</param>
    /// <returns>Parsed SimpleCdmEntity object</returns>
    public static SimpleCdmEntity FromJson(JsonDocument document)
    {
        var entityRoot = document.RootElement.GetProperty("definitions").EnumerateArray()
            .FirstOrDefault(prop => prop.TryGetProperty("entityName", out _));
        var complexTypes = document.RootElement.GetProperty("definitions").EnumerateArray()
            .Where(prop => prop.TryGetProperty("dataTypeName", out _));
        var entity = new SimpleCdmEntity
        {
            EntityName = entityRoot.GetProperty("entityName").GetString(),
            VersionNumber = document.RootElement
                .GetArrayElement("definitions", "exhibitsTraits")
                .FilterArray("traitReference", "is.CDM.entityVersion")
                .GetArrayElement("arguments", "name", "versionNumber")
                .GetProperty("value").GetString(),
            Attributes = entityRoot.GetProperty("hasAttributes").Deserialize<SimpleCdmAttribute[]>()
        };

        entity.Attributes = entity.Attributes.ResolveComplexTypes(complexTypes).ToArray();

        return entity;
    }

    /// <summary>
    /// Create DataReader for the entity
    /// </summary>
    /// <param name="mergeColumnName">Column to merge by</param>
    /// <returns>DataReader instance</returns>
    public IDataReader GetReader(string mergeColumnName)
    {
        var dt = new DataTable();

        foreach (var attr in Attributes)
            dt.Columns.Add(new DataColumn(attr.Name, SimpleCdmAttribute.MapCdmType(attr.DataType)));

        dt.Columns.Add(new DataColumn(mergeColumnName, typeof(string)));

        return dt.CreateDataReader();
    }

    private sealed class SimpleCdmEntityEqualityComparer : IEqualityComparer<SimpleCdmEntity>
    {
        public bool Equals(SimpleCdmEntity x, SimpleCdmEntity y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (ReferenceEquals(x, null)) return false;
            if (ReferenceEquals(y, null)) return false;
            if (x.GetType() != y.GetType()) return false;
            return x.EntityName == y.EntityName
                   && x.VersionNumber == y.VersionNumber
                   && x.Attributes.SequenceEqual(y.Attributes, SimpleCdmAttribute.SimpleCdmAttributeComparer);
        }

        public int GetHashCode(SimpleCdmEntity obj)
        {
            return HashCode.Combine(obj.EntityName, obj.VersionNumber, obj.Attributes);
        }
    }
}
