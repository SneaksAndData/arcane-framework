using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks;
using Arcane.Framework.Tests.Fixtures;
using Moq;
using Parquet.Data;
using Snd.Sdk.Storage.Models;
using Xunit;

namespace Arcane.Framework.Tests.SinkTests;

public class ParquetSinkTests : IClassFixture<AkkaFixture>, IClassFixture<ServiceFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly ServiceFixture serviceFixture;

    public ParquetSinkTests(AkkaFixture akkaFixture, ServiceFixture serviceFixture)
    {
        this.akkaFixture = akkaFixture;
        this.serviceFixture = serviceFixture;
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
        this.serviceFixture.MockBlobStorageService.Setup(mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(),
                It.Is<string>(p => p.Contains(pathString)), It.IsAny<string>(), It.IsAny<bool>()))
            .ReturnsAsync(new UploadedBlob());

        await Source.From(Enumerable.Range(0, blocks).Select(_ => columns.ToList())).RunWith(
            ParquetSink.Create(schema, this.serviceFixture.MockBlobStorageService.Object, $"tmp@{pathString}",
                rowGroupsPerBlock, createSchemaFile, dropCompletionToken: dropCompletionToken),
            this.akkaFixture.Materializer);

        this.serviceFixture.MockBlobStorageService.Verify(
            mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(), It.Is<string>(path => path.Contains(pathString)),
                It.Is<string>(fn => fn.StartsWith("part-")), It.IsAny<bool>()),
            createSchemaFile
                ? Times.Exactly(blocks / rowGroupsPerBlock + 2)
                : Times.Exactly(blocks / rowGroupsPerBlock + 1));
        this.serviceFixture.MockBlobStorageService.Verify(
            mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(), It.Is<string>(path => path.Contains(pathString)),
                It.Is<string>(fn => fn.StartsWith("schema-")), It.IsAny<bool>()),
            createSchemaFile ? Times.Exactly(1) : Times.Exactly(0));
        if (dropCompletionToken)
        {
            this.serviceFixture.MockBlobStorageService.Verify(
                mb => mb.SaveBytesAsBlob(It.IsAny<BinaryData>(), It.Is<string>(path => path.Contains(pathString)),
                    It.Is<string>(fn => fn.EndsWith(".COMPLETED")), It.IsAny<bool>()), Times.Exactly(1));
        }
    }
}
