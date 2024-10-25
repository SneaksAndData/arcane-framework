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
                    Description = "date_month",
                    FieldName = "my_column_with_date",
                    FieldFormat = "datetime"
                },
                new StreamPartition
                {
                    Description = "date_month",
                    FieldName = "",
                    FieldFormat = "",
                    FieldExpression = "date_format(cast('test' as date), 'yyyMM')",
                    IsDatePartition = true
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
            """[{"description":"date_month","field_name":"my_column_with_date","field_format":"datetime","field_expression":null,"is_date_partition":false},{"description":"date_month","field_name":"","field_format":"","field_expression":"date_format(cast(\u0027test\u0027 as date), \u0027yyyMM\u0027)","is_date_partition":true}]""";
        this.mockBlobStorageService.Verify(m => m.SaveTextAsBlob(expectedMetadata, $"{basePath}/metadata", "v0/partitions.json"), Times.Once);
    }

    [Fact]
    public async Task HandleSchemaFailures()
    {
        this.mockBlobStorageService
            .Setup(s => s.SaveBytesAsBlob(It.IsAny<BinaryData>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<bool>()))
            .ThrowsAsync(new Exception());

        var columns = Enumerable.Range(0, 10)
            .Select(col => new DataColumn(new DataField<int?>(col.ToString()), Enumerable.Range(0, 10).ToArray()))
            .ToArray();

        var schema = new Schema(columns.Select(c => c.Field).ToList());
        var source = Source.From(Enumerable.Range(0, 10).Select(_ => columns.ToList()));

        var sink = ParquetSink.Create(parquetSchema: schema,
            storageWriter: this.mockBlobStorageService.Object,
            parquetFilePath: "s3a://bucket/object",
            streamMetadata: new StreamMetadata(Option<StreamPartition[]>.None),
            createSchemaFile: true,
            dropCompletionToken: true);

        await Assert.ThrowsAsync<Exception>(async () => await source.RunWith(sink, this.akkaFixture.Materializer));
    }
}
