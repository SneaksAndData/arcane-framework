using System;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Sources.BlobStorage;
using Moq;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Xunit;

namespace Arcane.Framework.Tests.Sources;

public class BlobStorageSourceTests: IDisposable
{
    private readonly ActorSystem actorSystem = ActorSystem.Create(nameof(BlobStorageSourceTests));
    private readonly Mock<IBlobStorageService> mockBlobStorageService = new();
    private readonly CancellationTokenSource cts = new();

    [Fact]
    public async Task TestCanStreamBlobStorageObjectNames()
    {
        // Arrange
        this.mockBlobStorageService
            .Setup(s => s.ListBlobsAsEnumerable(It.IsAny<string>()))
            .Returns(new[] { new StoredBlob { Name = "key/value/item.csv", LastModified = DateTimeOffset.UtcNow } });
        var blobStorageSource = BlobStorageSource.Create("container",
            "",
            this.mockBlobStorageService.Object,
            TimeSpan.FromMinutes(1));
        var source = Source.FromGraph(blobStorageSource);

        // Act
        var result = await source
            .TakeWithin(TimeSpan.FromSeconds(5))
            .RunAggregate(0, (i, _) => ++i, this.actorSystem.Materializer());

        // Assert
        Assert.Equal(1, result);
    }

    [Fact]
    public async Task TestDoesNotStopOnEmpty()
    {
        // Arrange
        var token = this.cts.Token;
        var callCount = 0;
        this.mockBlobStorageService
            .Setup(s => s.ListBlobsAsEnumerable(It.IsAny<string>()))
            .Returns(() =>
            {
                if (callCount == 0)
                {
                    callCount++;
                    return new[] { new StoredBlob { Name = "key/value/item.csv", LastModified = DateTimeOffset.UtcNow } };
                }

                if (callCount > 3)
                {
                    this.cts.Cancel();
                }

                callCount++;
                return Array.Empty<StoredBlob>();
            });
        var blobStorageSource = BlobStorageSource.Create("container",
            "",
            this.mockBlobStorageService.Object,
            TimeSpan.FromSeconds(1));
        var source = Source.FromGraph(blobStorageSource);

        // Act
        var result = await source
            .TakeWithin(TimeSpan.FromMinutes(1))
            .Via(token.AsFlow<string>(cancelGracefully: true))
            .RunAggregate(0, (i, _) => ++i, this.actorSystem.Materializer());

        // Assert
        Assert.Equal(1, result);
        Assert.True(token.IsCancellationRequested);
        this.mockBlobStorageService
            .Verify(s => s.ListBlobsAsEnumerable(It.IsAny<string>()), Times.AtLeast(4));
    }

    public void Dispose()
    {
        this.actorSystem?.Dispose();
        this.cts?.Dispose();
    }
}
