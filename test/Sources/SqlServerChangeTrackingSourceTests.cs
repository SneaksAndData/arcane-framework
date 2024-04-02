using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.Extensions;
using Arcane.Framework.Sources.SqlServer;
using Arcane.Framework.Tests.Fixtures;
using Microsoft.Data.SqlClient;
using xRetry;
using Xunit;

namespace Arcane.Framework.Tests.Sources;

public class SqlServerChangeTrackingSourceTests : IClassFixture<ServiceFixture>, IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly string connectionString;
    private readonly ServiceFixture serviceFixture;

    public SqlServerChangeTrackingSourceTests(ServiceFixture serviceFixture, AkkaFixture akkaFixture)
    {
        this.serviceFixture = serviceFixture;
        this.akkaFixture = akkaFixture;
        this.connectionString =
            this.serviceFixture.PrepareSqlServerDatabase(nameof(SqlServerChangeTrackingSourceTests));
    }

    [RetryTheory(typeof(SqlException))]
    [InlineData(true, 101)]
    [InlineData(false, 1)]
    public async Task GetChanges(bool fullLoadOnStart, int expectedRows)
    {
        var source = SqlServerChangeTrackingSource.Create(
            this.connectionString,
            "dbo",
            nameof(SqlServerChangeTrackingSourceTests),
            fullLoadOnStart: fullLoadOnStart,
            lookBackRange: 1,
            changeCaptureInterval: TimeSpan.FromSeconds(30),
            streamKind: "test");
        var result = await Source.FromGraph(source)
            .TakeWithin(TimeSpan.FromSeconds(5))
            .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

        Assert.Equal(expectedRows, result.Count);
    }

    [RetryTheory(typeof(SqlException))]
    [InlineData(true, true, 101)]
    [InlineData(false, true, 102)]
    [InlineData(false, false, 2)]
    public async Task StopAfterFullLoad(bool stopAfterFullLoad, bool fullLoadOnStart, int expectedRows)
    {
        var task = Source.FromGraph(SqlServerChangeTrackingSource.Create(
                this.connectionString,
                "dbo",
                nameof(SqlServerChangeTrackingSourceTests),
                fullLoadOnStart: fullLoadOnStart,
                stopAfterFullLoad: stopAfterFullLoad,
                lookBackRange: 1,
                changeCaptureInterval: TimeSpan.FromSeconds(1),
                streamKind: "test"))
            .TakeWithin(TimeSpan.FromSeconds(5))
            .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);
        await Task.Delay(1000);
        this.serviceFixture.InsertChanges(nameof(SqlServerChangeTrackingSourceTests));
        var result = await task;
        Assert.Equal(expectedRows, result.Count);
    }

    [Theory]
    [InlineData(2, "cc2b36e9f63cdbf5baa90d8eeb0d8edfae3017b137ff2c94ef9d9436bac760f0")]
    public async Task GetDeletes(int expectedRows, string expectedMergeKey)
    {
        var task = Source.FromGraph(SqlServerChangeTrackingSource.Create(
                this.connectionString,
                "dbo",
                nameof(SqlServerChangeTrackingSourceTests),
                lookBackRange: 1,
                changeCaptureInterval: TimeSpan.FromSeconds(1),
                streamKind: "test"))
            .TakeWithin(TimeSpan.FromSeconds(5))
            .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);
        await Task.Delay(1000);
        this.serviceFixture.DeleteChanges(nameof(SqlServerChangeTrackingSourceTests));
        var result = await task;
        Assert.Equal(expectedRows, result.Count);
        Assert.Equal(expectedMergeKey, result[1].Find(c => c.FieldName == Constants.UPSERT_MERGE_KEY).Value);
    }

    [Theory]
    [InlineData(true, false)]
    public async Task StopAfterFullLoadException(bool stopAfterFullLoad, bool fullLoadOnStart)
    {
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            _ = await Source.FromGraph(SqlServerChangeTrackingSource.Create(
                    this.connectionString,
                    "dbo",
                    nameof(SqlServerChangeTrackingSourceTests),
                    fullLoadOnStart: fullLoadOnStart,
                    stopAfterFullLoad: stopAfterFullLoad,
                    lookBackRange: 1,
                    changeCaptureInterval: TimeSpan.FromSeconds(30),
                    streamKind: "test"))
                .TakeWithin(TimeSpan.FromSeconds(5))
                .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);
        });
        Assert.Equal("fullLoadOnStart must be true if stopAfterFullLoad is set to true",
            exception.Message);
    }

    [Fact]
    public void GetParquetSchema()
    {
        var schema = SqlServerChangeTrackingSource
            .Create(this.connectionString, "dbo", nameof(SqlServerChangeTrackingSourceTests), "test")
            .GetParquetSchema();

        Assert.Equal(6,
            schema.Fields
                .Count); // two base columns, SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION, ChangeTrackingVersion and ARCANE_MERGE_KEY
    }

    [Fact]
    public void GetColumns()
    {
        var inputCols = new Dictionary<string, string>
        {
            { "colA", "int not null" },
            { "colB", "nvarchar(10)" },
            { "SYS_CHANGE_VERSION", "int" },
            { "colC", "bigint" }
        };
        this.serviceFixture.CreateTable(this.connectionString, nameof(GetColumns), inputCols, "colA");

        using var sqlCon = new SqlConnection(this.connectionString);
        sqlCon.Open();

        var result = SqlServerUtils.GetColumns("dbo", nameof(GetColumns), sqlCon)
            .ToDictionary(c => c.columnName, c => c.isPrimaryKey);

        sqlCon.Close();
        sqlCon.Dispose();

        Assert.Contains(result, e => e is { Key: "SYS_CHANGE_VERSION", Value: false });
    }
}
