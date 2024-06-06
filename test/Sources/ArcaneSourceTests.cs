using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Extensions;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.Base;
using Arcane.Framework.Sources.Exceptions;
using Arcane.Framework.Sources.Extensions;
using Arcane.Framework.Sources.SqlServer;
using Arcane.Framework.Tests.Fixtures;
using Parquet.Data;
using Xunit;
using ParquetColumn = Parquet.Data.DataColumn;

namespace Arcane.Framework.Tests.Sources;

public class ArcaneSourceTests : IClassFixture<AkkaFixture>
{
    private readonly IMaterializer materializer;

    public ArcaneSourceTests(AkkaFixture akkaFixture)
    {
        this.materializer = akkaFixture.Materializer;
    }

    [Fact]
    public async Task TestSchemaFreeSource()
    {
        // Arrange
        var rows = new[]
        {
            new DataCell("name", typeof(string), "John"),
            new DataCell("familyName", typeof(string), "Doe"),
            new DataCell("birthday", typeof(DateTimeOffset), DateTimeOffset.Now)
        };
        var brokenRows = new[]
        {
            new DataCell("name", typeof(string), "John")
        };
        var parquetSchema = new Schema(rows.Select(df => new DataField(df.FieldName, df.FieldType)).ToList());
        var mockSource = Source.From(Enumerable.Range(0, 100)
            .Select(idx => idx % 10 == 0 ? brokenRows.ToList() : rows.ToList()));
        var sink = Sink.Aggregate<List<ParquetColumn>, int>(0, (acc, _) => acc + 1);

        // Act
        var sfs = mockSource
            .ToArcaneSource()
            .MapSource(s => s.GroupedWithin(10, TimeSpan.FromSeconds(10)))
            .Map(grp => grp.ToList().ToRowGroup(parquetSchema))
            .To(sink.ToArcaneSink());

        var result = await sfs.Run(this.materializer);

        // Assert
        Assert.Equal(10, result);
    }

    [Fact]
    public async Task TestSchemaBoundSource()
    {
        // Arrange
        var rows = new[]
        {
            new DataCell("name", typeof(string), "John"),
            new DataCell("familyName", typeof(string), "Doe"),
            new DataCell("birthday", typeof(DateTimeOffset), DateTimeOffset.Now)
        };
        var parquetSchema = new Schema(rows.Select(df => new DataField(df.FieldName, df.FieldType)).ToList());

        var brokenRows = new[]
        {
            new DataCell("familyName", typeof(string), "Doe"),
            new DataCell("familyName", typeof(string), "Doe")
        };
        var brokenSchema = new Schema(rows.Take(2).Select(df => new DataField(df.FieldName, df.FieldType)).ToList());

        var mockSource =
            Source.From(Enumerable.Range(0, 100).Select(idx => idx > 9 ? brokenRows.ToList() : rows.ToList()));
        var sink = Sink.Aggregate<List<ParquetColumn>, int>(0, (acc, _) => acc + 1);
        var validator = new FastParquetSchemaValidator(parquetSchema);

        var callCount = 0;
        // Act
        var sfs = mockSource
            .ToArcaneSource()
            .MapSource(s => s.GroupedWithin(10, TimeSpan.FromSeconds(10)))
            .Map(grp =>
            {
                var schema = callCount < 1 ? parquetSchema : brokenSchema;
                callCount++;
                return grp.ToList().ToRowGroup(schema);
            })
            .WithSchema(validator)
            .To(sink.ToArcaneSink().WithSchema(validator));

        var ex = await Assert.ThrowsAsync<SchemaInconsistentException>(async () => await sfs.Run(this.materializer));

        // Assert
        Assert.Contains("Source has 2 fields, sink has 3 fields", ex.Message);
    }
}
