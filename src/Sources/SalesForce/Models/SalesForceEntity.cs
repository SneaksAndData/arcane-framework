using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;

namespace Arcane.Framework.Sources.SalesForce.Models;

/// <summary>
/// Represents CDM Change Feed entity
/// </summary>
public class SalesForceEntity
{
    /// <summary>
    /// Entity name
    /// </summary>
    public string EntityName { get; set; }


    /// <summary>
    /// Attributes collection
    /// </summary>
    public SalesForceAttribute[] Attributes { get; set; }

    /// <summary>
    /// Comparer class
    /// </summary>
    public static IEqualityComparer<SalesForceEntity> SalesForceEntityComparer { get; } =
        new SalesForceEntityEqualityComparer();

    /// <summary>
    /// Parse CDM entity from a JSON document
    /// </summary>
    /// <param name="document">Json document to parse</param>
    /// <returns>Parsed SimpleCdmEntity object</returns>
    public static SalesForceEntity FromJson(string entityName, JsonDocument document)
    {
        var entity = new SalesForceEntity
        {
            EntityName = entityName,
            Attributes = document.RootElement.GetProperty("records").Deserialize<SalesForceAttribute[]>()
        };


        return entity;
    }

    /// <summary>
    /// Create DataReader for the entity
    /// </summary>
    /// <param name="mergeColumnName">Column to merge by</param>
    /// <returns>DataReader instance</returns>
    public IDataReader GetReader()
    {
        var dt = new DataTable();

        foreach (var attr in this.Attributes)
        {
            dt.Columns.Add(new DataColumn(attr.Name, SalesForceAttribute.MapSalesforceType(attr.DataType)));
        }

        // dt.Columns.Add(new DataColumn(mergeColumnName, typeof(string)));

        return dt.CreateDataReader();
    }

    private sealed class SalesForceEntityEqualityComparer : IEqualityComparer<SalesForceEntity>
    {
        public bool Equals(SalesForceEntity x, SalesForceEntity y)
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

            return x.EntityName == y.EntityName
                   && x.Attributes.SequenceEqual(y.Attributes, SalesForceAttribute.SalesForceAttributeComparer);
        }

        public int GetHashCode(SalesForceEntity obj)
        {
            return HashCode.Combine(obj.EntityName, obj.Attributes);
        }
    }
}
