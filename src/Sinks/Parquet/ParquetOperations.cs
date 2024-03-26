using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using Arcane.Framework.Sinks.Parquet.Exceptions;
using Arcane.Framework.Sinks.Parquet.Models;
using Microsoft.OpenApi.Extensions;
using Microsoft.OpenApi.Models;
using Parquet;
using Parquet.Data;
using ParquetColumn = Parquet.Data.DataColumn;

namespace Arcane.Framework.Sinks.Parquet;

/// <summary>
/// Operations for working with Parquet files
/// </summary>
public static class ParquetOperations
{
    /// <summary>
    /// Converts OpenAPI schema to Parquet schema
    /// </summary>
    /// <param name="apiSchema">OpenAPI schema</param>
    /// <returns>Parquet schema</returns>
    public static Schema ToParquetSchema(this OpenApiSchema apiSchema)
    {
        var fields = apiSchema.Properties.Select(ResolveObjectProperty).ToList();

        return new Schema(fields);
    }


    /// <summary>
    /// Converts IDataReader to Parquet schema
    /// </summary>
    /// <param name="reader">The IDataReader instance</param>
    /// <returns>Parquet schema</returns>
    public static Schema ToParquetSchema(this IDataReader reader)
    {
        var fields = Enumerable.Range(0, reader.FieldCount)
            .Select(ixCol =>
            {
                var fieldType = reader.GetFieldType(ixCol).GetNullableClrType();
                return new DataField(reader.GetName(ixCol), fieldType);
            })
            .ToList();

        return new Schema(fields);
    }

    /// <summary>
    /// Converts a list of data cells to a Parquet row group
    /// </summary>
    /// <param name="cellGroups">List of data cells</param>
    /// <param name="parquetSchema">Parquet Schema for a column</param>
    /// <returns>List of the Parquet columns</returns>
    /// <exception cref="SchemaInconsistentException">Thrown if the schema of the source is inconsistent with the schema of the sink</exception>
    public static List<ParquetColumn> AsRowGroup(this List<List<DataCell>> cellGroups, Schema parquetSchema)
    {
        if (cellGroups.Count > 0)
        {
            var (srcFields, sinkFields) = (cellGroups.First().Count, parquetSchema.GetDataFields().Length);
            if (srcFields != sinkFields) throw new SchemaInconsistentException(srcFields, sinkFields);
        }

        var columns = parquetSchema.GetDataFields().Select((field, _) =>
                new ParquetColumn(field, Array.CreateInstance(field.ClrType.GetNullableClrType(), cellGroups.Count)))
            .ToList();
        foreach (var (cellGroup, ix_rec) in cellGroups.Select((grp, ix) => (grp, ix)))
        {
            foreach (var (cell, ix_col) in cellGroup.Select((c, ix) => (c, ix)))
            {

                if ((columns[ix_col].Field.ClrType == typeof(DateTimeOffset?) ||
                     columns[ix_col].Field.ClrType == typeof(DateTimeOffset)) && cell.FieldType == typeof(DateTime) &&
                    cell.Value != DBNull.Value && cell.Value != null)
                {
                    DateTimeOffset? newValue = DateTime.SpecifyKind((DateTime)cell.Value, DateTimeKind.Utc);
                    columns[ix_col].Data.SetValue(newValue, ix_rec);
                }
                else if (columns[ix_col].Field.ClrType == typeof(string) && cell.FieldType == typeof(Guid) &&
                         cell.Value != DBNull.Value && cell.Value != null)
                {
                    var newValue = ((Guid)cell.Value).ToString();
                    columns[ix_col].Data.SetValue(newValue, ix_rec);
                }
                else if ((columns[ix_col].Field.ClrType == typeof(long?) ||
                          columns[ix_col].Field.ClrType == typeof(long)) && cell.FieldType == typeof(int) &&
                         cell.Value != DBNull.Value && cell.Value != null)
                {
                    columns[ix_col].Data.SetValue((long?)(int)cell.Value, ix_rec);
                }
                else
                {
                    if (cell.Value != DBNull.Value) columns[ix_col].Data.SetValue(cell.Value, ix_rec);
                }
            }
        }
        return columns;
    }

    /// <summary>
    /// Gets the hash of the Parquet schema
    /// </summary>
    /// <param name="parquetSchema">Parquet schema</param>
    /// <returns>Schema hash sum</returns>
    public static (string fullhash, string shortHash, byte[] schemaBytes) GetSchemaHash(this Schema parquetSchema)
    {
        // write empty parquet file out
        var emptyStream = new MemoryStream();
        using (var parquetWriter = new ParquetWriter(parquetSchema, emptyStream))
        {
            parquetWriter.CustomMetadata = new Dictionary<string, string>
            {
                { Constants.UPSERT_MERGE_KEY_NAME, Constants.UPSERT_MERGE_KEY }
            };

            using (var groupWriter = parquetWriter.CreateRowGroup())
            {
                foreach (var parquetColumn in GetEmptyRowGroup(parquetSchema)) groupWriter.WriteColumn(parquetColumn);
            }
        }

        var schemaBytes = emptyStream.ToArray();
        var fullHash = Convert.ToBase64String(SHA256.HashData(schemaBytes)).ToLowerInvariant().Replace("/", "0");
        return (fullHash, fullHash[0..7], schemaBytes);
    }

    private static List<ParquetColumn> GetEmptyRowGroup(Schema parquetSchema)
    {
        return parquetSchema.GetDataFields().Select(field =>
            new ParquetColumn(field, Array.CreateInstance(field.ClrType.GetNullableClrType(), 0))).ToList();
    }

    private static Type GetNullableClrType(this Type clrType)
    {
        if (clrType == typeof(Guid)) return typeof(string);

        if (clrType == typeof(string) || clrType.IsArray) return clrType;

        return typeof(Nullable<>).MakeGenericType(clrType);
    }

    private static Field ResolveObjectProperty(this KeyValuePair<string, OpenApiSchema> objectProperty)
    {
        var propertyType = objectProperty.Value.MapOpenApiPrimitiveTypeToSimpleType();
        if (propertyType != typeof(object)) return new DataField(objectProperty.Key, propertyType.GetNullableClrType());

        return new StructField(objectProperty.Key,
            objectProperty.Value.Properties.Select(prop => prop.ResolveObjectProperty()).ToArray());
    }

}
