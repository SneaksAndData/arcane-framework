using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.SqlServer;
using Arcane.Framework.Tests.Fixtures;
using Xunit;

namespace Arcane.Framework.Tests.Sources;

public class SqlServerSourceTests : IClassFixture<ServiceFixture>, IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly string connectionString;
    private readonly ServiceFixture serviceFixture;

    public SqlServerSourceTests(ServiceFixture serviceFixture, AkkaFixture akkaFixture)
    {
        this.serviceFixture = serviceFixture;
        this.akkaFixture = akkaFixture;
        this.connectionString = this.serviceFixture.PrepareSqlServerDatabase(nameof(SqlServerSourceTests));
    }

    [Fact]
    public async Task Test()
    {
        var result = await Source
            .FromGraph(SqlServerSource.Create(this.connectionString, "dbo", nameof(SqlServerSourceTests), "test"))
            .TakeWithin(TimeSpan.FromSeconds(3))
            .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

        Assert.Equal(101, result.Count);
    }

    [Fact]
    public void GetParquetSchema()
    {
        var schema = SqlServerSource.Create(this.connectionString, "dbo", nameof(SqlServerSourceTests), "test")
            .GetParquetSchema();

        Assert.Equal(2, schema.Fields.Count); // two base columns
    }
}
