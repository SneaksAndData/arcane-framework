using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Json;
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
}
