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
    public async Task TestPullChanges()
    {
        // Arrange
        var source = SqlServerSource.Create(this.connectionString,
            "dbo",
            nameof(SqlServerSourceTests),
            TimeSpan.FromSeconds(1));

        // Act
        var result = await Source
            .FromGraph(source)
            .TakeWithin(TimeSpan.FromSeconds(10))
            .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

        // Assert
        // Expect to receive rows at least two times since the source reschedules polling every second.
        // And not more then 10 times since we are taking within 10 seconds interval.
        Assert.InRange(result.Count, 101 * 2, 101 * 10);
    }

    [Fact]
    public void GetParquetSchema()
    {
        // Arrange
        var source = SqlServerSource.Create(this.connectionString,
            "dbo",
            nameof(SqlServerSourceTests),
            TimeSpan.FromSeconds(1));

        // Act
        var schema = source.GetParquetSchema();

        // Assert
        Assert.Equal(2, schema.Fields.Count); // two base columns
    }

    [Fact]
    public async Task ChangeCaptureInterval()
    {
        // Arrange
        var source = SqlServerSource.Create(this.connectionString,
            "dbo",
            nameof(SqlServerSourceTests),
            TimeSpan.FromSeconds(5));

        // Act
        var result = await Source
            .FromGraph(source)
            .TakeWithin(TimeSpan.FromSeconds(15))
            .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

        // Assert
        Assert.InRange(result.Count, 101, 101 * 3);
    }
}
