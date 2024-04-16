using System;
using System.Collections.Generic;
using System.IO;
using Arcane.Framework.Sinks.Parquet;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Models;
using Microsoft.OpenApi.Readers;
using Parquet.Data;
using Xunit;

namespace Arcane.Framework.Tests.Operations;

public class ParquetOperationsTests
{
    public static IEnumerable<object[]> GenerateParquetSchemas => new List<object[]>
    {
        new object[]
        {
            "open_api_schema_simple.yaml", new Schema(
                new DataField("page", DataType.Int32),
                new DataField("limit", DataType.Int32),
                new DataField("pages", DataType.Int32),
                new DataField("total", DataType.Int32))
        },
        new object[]
        {
            "open_api_schema_nested.yaml", new Schema(
                new DataField("page", DataType.Int32),
                new StructField("limit", new DataField("max_pages", DataType.Int32),
                    new DataField("min_pages", DataType.Int32)),
                new DataField("desc", DataType.String))
        },
        new object[]
        {
            "open_api_schema_nested_two_levels.yaml", new Schema(
                new DataField("page", DataType.Int32),
                new StructField("limit", new DataField("max_pages", DataType.Int32),
                    new DataField("min_pages", DataType.Int32),
                    new StructField("capacity", new DataField("size", DataType.Int64))),
                new DataField("desc", DataType.String))
        }
    };

    [Theory]
    [MemberData(nameof(GenerateParquetSchemas))]
    public void GetParquetFromReader(string schemaName, Schema expectedSchema)
    {
        var schema = File.ReadAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Operations",
            "RestApiTestData", schemaName));

        var openApiStringReader = new OpenApiStringReader();
        var openApiSchema =
            openApiStringReader.ReadFragment<OpenApiSchema>(schema, OpenApiSpecVersion.OpenApi3_0, out var _);

        var parquetSchema = openApiSchema.ToParquetSchema();

        Assert.Equal(expectedSchema.GetDataFields(), parquetSchema.GetDataFields());
    }
}
