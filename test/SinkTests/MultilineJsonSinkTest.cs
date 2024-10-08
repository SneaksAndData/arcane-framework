using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Framework.Sinks.Json;
using Arcane.Framework.Sinks.Models;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Tests.Fixtures;
using Moq;
using Parquet.Data;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Xunit;

namespace Arcane.Framework.Tests.SinkTests;

public class MultilineJsonSinkTest : IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly Mock<IBlobStorageService> mockBlobStorageService = new();

    public MultilineJsonSinkTest(AkkaFixture akkaFixture)
    {
        this.akkaFixture = akkaFixture;
    }

    [Theory]
    [InlineData(1, 1, false)]
    [InlineData(1, 10, false)]
    [InlineData(1, 0, false)]
    [InlineData(3, 5, false)]
    [InlineData(1, 10, true)]
    public async Task MultilineJsonSinkWrites(int files, int rowsPerFile, bool dropsCompletion)
    {
        var mockPath = $"tmp@json/{Guid.NewGuid()}";
        var mockSchema = new Schema(new DataField("test", DataType.Int32));
        var (fullHash, shortHash, schemaBytes) = mockSchema.GetSchemaHash();
        var mockIn = Enumerable
            .Range(0, files)
            .Select(_ => Enumerable.Range(0, rowsPerFile).Select(ix =>
                JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(new { Value = ix }))))
            .ToList();

        this.mockBlobStorageService.Setup(mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(),
                It.Is<string>(p => p == mockPath), It.IsAny<string>(), It.IsAny<bool>()))
            .ReturnsAsync(new UploadedBlob());

        await Source
            .From(mockIn)
            .Select(v => v.ToList())
            .RunWith(MultilineJsonSink.Create(this.mockBlobStorageService.Object, mockPath, mockSchema,
                    new StreamMetadata(Option<StreamPartition[]>.None),
                    "data", "schema", dropsCompletion), this.akkaFixture.Materializer);

        foreach (var _ in Enumerable.Range(0, files))
        {
            this.mockBlobStorageService.Verify(mb => mb.SaveBytesAsBlob(
                    It.Is<BinaryData>(bd =>
                        (bd.ToArray().Length > 0 ? bd.ToString() : string.Empty)
                        .Split(Environment.NewLine, StringSplitOptions.None).Length - 1 == rowsPerFile),
                    It.Is<string>(path => path == $"{mockPath}/data"),
                    It.Is<string>(fn => fn.StartsWith("part-")),
                    It.IsAny<bool>()),
                Times.Exactly(rowsPerFile > 0 ? files : 0));
        }

        this.mockBlobStorageService.Verify(mb => mb.SaveBytesAsBlob(
                It.Is<BinaryData>(bd => bd.ToArray().SequenceEqual(schemaBytes)),
                It.Is<string>(path => path == $"{mockPath}/schema"),
                It.Is<string>(fn => fn.EndsWith($"{shortHash}.parquet")),
                It.IsAny<bool>()),
            Times.Once);

        if (dropsCompletion)
        {
            this.mockBlobStorageService.Verify(mb => mb.SaveBytesAsBlob(
                    It.Is<BinaryData>(bd => bd.ToArray().SequenceEqual(new byte[] { 0 })),
                    It.Is<string>(path => path == $"{mockPath}/data"),
                    It.Is<string>(fn => fn == $"{shortHash}.COMPLETED"),
                    It.IsAny<bool>()),
                Times.Once);
        }
    }

    [Fact]
    public async Task RemovesEmptyStreamMetadata()
    {
        var basePath = "s3a://bucket/path";
        var mockIn = Enumerable
            .Range(0, 10)
            .Select(_ => Enumerable.Range(0, 1).Select(ix => JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(new { Value = ix }))))
            .ToList();
        var schema = new Schema(new DataField("test", DataType.Int32));

        var sink = MultilineJsonSink.Create(this.mockBlobStorageService.Object,
            basePath,
            schema,
            new StreamMetadata(Option<StreamPartition[]>.None));

        await Source.From(mockIn).Select(v => v.ToList()).RunWith(sink, this.akkaFixture.Materializer);

        this.mockBlobStorageService.Verify(m => m.RemoveBlob($"{basePath}/metadata", "partitions.json"), Times.Once);
    }

    [Fact]
    public async Task OverwritesExistingSchemaMetadata()
    {
        var basePath = "s3a://bucket/path";
        var mockIn = Enumerable
            .Range(0, 10)
            .Select(_ => Enumerable.Range(0, 1).Select(ix => JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(new { Value = ix }))))
            .ToList();
        var schema = new Schema(new DataField("test", DataType.Int32));

        var metadata = new StreamMetadata(
            new[]
            {
                new StreamPartition
                {
                    Name = "date",
                    FieldName = "my_column_with_date",
                    FieldFormat = "datetime"
                },
                new StreamPartition
                {
                    Name = "sales_organisation",
                    FieldName = "my_column_with_sales_org",
                    FieldFormat = "string"
                }
            });
        var sink = MultilineJsonSink.Create(this.mockBlobStorageService.Object,
            basePath,
            schema,
            metadata);

        await Source.From(mockIn).Select(v => v.ToList()).RunWith(sink, this.akkaFixture.Materializer);

        var expectedMetadata =
            """[{"name":"date","field_name":"my_column_with_date","field_format":"datetime"},{"name":"sales_organisation","field_name":"my_column_with_sales_org","field_format":"string"}]""";
        this.mockBlobStorageService.Verify(m => m.SaveTextAsBlob(expectedMetadata, $"{basePath}/metadata", "partitions.json"), Times.Once);
    }
}
