using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Framework.Sinks.Models;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Tests.Fixtures;
using Moq;
using Parquet.Data;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Xunit;

namespace Arcane.Framework.Tests.SinkTests;

public class ParquetSinkTests : IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly Mock<IBlobStorageService> mockBlobStorageService = new();

    public ParquetSinkTests(AkkaFixture akkaFixture)
    {
        this.akkaFixture = akkaFixture;
    }

    [Theory]
    [InlineData(10, 1, false, false)]
    [InlineData(10, 2, false, false)]
    [InlineData(30, 4, false, false)]
    [InlineData(10, 1, true, false)]
    [InlineData(10, 1, true, true)]
    public async Task ParquetSinkWrites(int blocks, int rowGroupsPerBlock, bool createSchemaFile,
        bool dropCompletionToken)
    {
        var columns = Enumerable.Range(0, 10).Select(ix_col =>
        {
            var field = new DataField<int?>(ix_col.ToString());
            return new DataColumn(field, Enumerable.Range(0, 10).ToArray());
        });

        var pathString = Guid.NewGuid().ToString();

        var schema = new Schema(columns.Select(c => c.Field).ToList());
        this.mockBlobStorageService.Setup(mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(),
                It.Is<string>(p => p.Contains(pathString)), It.IsAny<string>(), It.IsAny<bool>()))
            .ReturnsAsync(new UploadedBlob());

        await Source.From(Enumerable.Range(0, blocks).Select(_ => columns.ToList())).RunWith(
            ParquetSink.Create(schema, this.mockBlobStorageService.Object, $"tmp@{pathString}",
                new StreamMetadata(Option<StreamPartition[]>.None),
                rowGroupsPerBlock, createSchemaFile, dropCompletionToken: dropCompletionToken),
            this.akkaFixture.Materializer);

        this.mockBlobStorageService.Verify(
            mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(), It.Is<string>(path => path.Contains(pathString)),
                It.Is<string>(fn => fn.StartsWith("part-")), It.IsAny<bool>()),
            createSchemaFile
                ? Times.Exactly(blocks / rowGroupsPerBlock + 2)
                : Times.Exactly(blocks / rowGroupsPerBlock + 1));
        this.mockBlobStorageService.Verify(
            mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(), It.Is<string>(path => path.Contains(pathString)),
                It.Is<string>(fn => fn.StartsWith("schema-")), It.IsAny<bool>()),
            createSchemaFile ? Times.Exactly(1) : Times.Exactly(0));
        if (dropCompletionToken)
        {
            this.mockBlobStorageService.Verify(
                mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(), It.Is<string>(path => path.Contains(pathString)),
                    It.Is<string>(fn => fn.EndsWith(".COMPLETED")), It.IsAny<bool>()), Times.Exactly(1));
        }
    }

    [Fact]
    public async Task RemovesEmptyStreamMetadata()
    {
        var basePath = "s3a://bucket/path";
        var columns = Enumerable
            .Range(0, 10)
            .Select(col => new DataColumn(new DataField<int?>(col.ToString()), Enumerable.Range(0, 10).ToArray()))
            .ToList();
        var schema = new Schema(columns.Select(c => c.Field).ToList());

        var sink = ParquetSink.Create(schema,
            this.mockBlobStorageService.Object,
            basePath,
            new StreamMetadata(Option<StreamPartition[]>.None),
            5,
            true,
            false);

        await Source.From(Enumerable.Range(0, 10).Select(_ => columns.ToList())).RunWith(sink, this.akkaFixture.Materializer);

        this.mockBlobStorageService.Verify(m => m.RemoveBlob($"{basePath}/metadata", "v0/partitions.json"), Times.Once);
    }

    [Fact]
    public async Task OverwritesExistingSchemaMetadata()
    {
        var basePath = "s3a://bucket/path";
        var columns = Enumerable
            .Range(0, 10)
            .Select(col => new DataColumn(new DataField<int?>(col.ToString()), Enumerable.Range(0, 10).ToArray()))
            .ToList();
        var schema = new Schema(columns.Select(c => c.Field).ToList());

        var metadata = new StreamMetadata(
            new[]
            {
                new StreamPartition
                {
                    Description = "date",
                    FieldName = "my_column_with_date",
                    FieldFormat = "datetime"
                },
                new StreamPartition
                {
                    Description = "sales_organisation",
                    FieldName = "my_column_with_sales_org",
                    FieldFormat = "string"
                }
            });
        var sink = ParquetSink.Create(schema,
            this.mockBlobStorageService.Object,
            basePath,
            metadata,
            5,
            true,
            false);

        await Source.From(Enumerable.Range(0, 10).Select(_ => columns.ToList())).RunWith(sink, this.akkaFixture.Materializer);

        var expectedMetadata =
            """[{"name":"date","field_name":"my_column_with_date","field_format":"datetime"},{"name":"sales_organisation","field_name":"my_column_with_sales_org","field_format":"string"}]""";
        this.mockBlobStorageService.Verify(m => m.SaveTextAsBlob(expectedMetadata, $"{basePath}/metadata", "v0/partitions.json"), Times.Once);
    }
}
