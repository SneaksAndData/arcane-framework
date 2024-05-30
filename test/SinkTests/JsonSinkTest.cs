using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Json;
using Arcane.Framework.Tests.Fixtures;
using Moq;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Xunit;

namespace Arcane.Framework.Tests.SinkTests;

public class JsonSinkTests : IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly Mock<IBlobStorageService> mockBlobStorageService = new();

    public JsonSinkTests(AkkaFixture akkaFixture)
    {
        this.akkaFixture = akkaFixture;
        this.mockBlobStorageService.Setup(mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>()))
            .ReturnsAsync(new UploadedBlob());

    }

    [Theory]
    [InlineData(10, 1)]
    [InlineData(10, 10)]
    public async Task JsonSinkWrites(int sources, int rowsPerSource)
    {
        var mockDocument =
            JsonSerializer.Deserialize<JsonDocument>(JsonSerializer.Serialize(new { test = 1, moreTest = "a" }));

        await Source
            .From(Enumerable.Range(0, sources).Select(ix =>
            {
                var values = new List<(DateTimeOffset, JsonDocument)>();
                foreach (var _ in Enumerable.Range(0, rowsPerSource))
                {
                    values.Add((DateTimeOffset.UtcNow, mockDocument));
                }

                return ($"test_{ix}", values);
            }))
            .RunWith(JsonSink.Create(this.mockBlobStorageService.Object, $"tmp@"),
                this.akkaFixture.Materializer);

        foreach (var ix_src in Enumerable.Range(0, sources))
        {
            this.mockBlobStorageService.Verify(
                mb => mb.SaveBytesAsBlob(
                    It.Is<BinaryData>(bd =>
                        bd.ToString().Split(Environment.NewLine, StringSplitOptions.None).Length - 1 == rowsPerSource),
                    It.Is<string>(path => path == $"tmp@test_{ix_src}"), It.Is<string>(fn => fn.StartsWith("part-")),
                    It.IsAny<bool>()), Times.Once);
        }
    }
}
