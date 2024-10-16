using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.CdmChangeFeedSource;
using Arcane.Framework.Sources.CdmChangeFeedSource.Exceptions;
using Arcane.Framework.Sources.Exceptions;
using Arcane.Framework.Tests.Extensions;
using Arcane.Framework.Tests.Fixtures;
using Moq;
using Parquet.Data;
using Snd.Sdk.Storage.Base;
using Snd.Sdk.Storage.Models;
using Xunit;

namespace Arcane.Framework.Tests.Sources
{
    public class CdmChangeFeedSourceTests : IClassFixture<AkkaFixture>
    {
        private readonly AkkaFixture akkaFixture;
        private readonly Mock<IBlobStorageService> mockBlobStorageService = new();

        public CdmChangeFeedSourceTests(AkkaFixture akkaFixture)
        {
            this.akkaFixture = akkaFixture;
        }

        [Theory]
        [InlineData(true, 11, "ValidEntity")]
        [InlineData(false, 8, "ValidEntity")]
        public async Task GetChanges(bool isBackfilling, int expectedRows, string entityName)
        {
            this.SetupTableMocks(entityName);


            var result = await Source.FromGraph(CdmChangeFeedSource.Create(rootPath: "test",
                    entityName: entityName,
                    blobStorage: this.mockBlobStorageService.Object,
                    isBackfilling: isBackfilling,
                    changeCaptureInterval: TimeSpan.FromSeconds(15)))
                .TakeWithin(TimeSpan.FromSeconds(5)).RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

            Assert.Equal(expectedRows, result.Count);
        }

        [Fact]
        public async Task GetChangesWithExceptions()
        {
            this.mockBlobStorageService
                .Setup(s => s.ListBlobsAsEnumerable(It.IsAny<string>()))
                .Returns(new[] { new StoredBlob { Name = "exceptionTest.csv", LastModified = DateTimeOffset.UtcNow } });

            this.mockBlobStorageService
                .Setup(mbs
                    => mbs.GetBlobContent(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Func<BinaryData, string>>()))
                .Throws(() => new OutOfMemoryException("Test exception"));

            var ex = await Assert.ThrowsAsync<OutOfMemoryException>(async () =>
            {
                _ = await Source.FromGraph(CdmChangeFeedSource.Create(rootPath: "test",
                        entityName: "ExceptionTest",
                        blobStorage: this.mockBlobStorageService.Object,
                        isBackfilling: false,
                        changeCaptureInterval: TimeSpan.FromSeconds(15)))
                    .TakeWithin(TimeSpan.FromSeconds(5))
                    .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

            });
            Assert.Equal("Test exception", ex.Message);
        }

        [Theory]
        [InlineData("updated")]
        [InlineData("column_order_changed")]
        [InlineData("extended")]
        public async Task SchemaChangeTests(string schemaUpdateName)
        {
            // Arrange
            const string entityName = "SchemaChangeTests";
            this.SetupTableMocks(entityName);
            this.SetupSchema(entityName, schemaUpdateName);
            var source = CdmChangeFeedSource.Create(rootPath: "test",
                entityName: entityName,
                blobStorage: this.mockBlobStorageService.Object,
                changeCaptureInterval: TimeSpan.FromSeconds(1),
                schemaUpdateInterval: TimeSpan.FromSeconds(1));

            // Act
            var ex = await Assert.ThrowsAsync<SchemaMismatchException>(async ()
                => await Source.FromGraph(source)
                    .TakeWithin(TimeSpan.FromSeconds(5))
                    .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer));

            // Assert
            Assert.Equal("Data source schema has been updated", ex.Message);
            this.mockBlobStorageService.Verify(s
                    => s.GetBlobContent("test", $"ChangeFeed/{entityName}.cdm.json",
                        It.IsAny<Func<BinaryData, JsonDocument>>()),
                Times.AtLeast(1)
            );
        }

        [Fact]
        public async Task MissingSchemaTests()
        {
            // Arrange
            const string entityName = "SchemaChangeTests";
            this.SetupTableMocks(entityName);
            var entityPath = Path.Join(
                AppDomain.CurrentDomain.BaseDirectory,
                "Sources",
                "SampleData",
                "BaseEntity",
                entityName,
                $"{entityName}.cdm.json");
            var callCount = 0;
            this.mockBlobStorageService
                .Setup(mbs => mbs.GetBlobContent(It.IsAny<string>(),
                    $"ChangeFeed/{entityName}.cdm.json",
                    It.IsAny<Func<BinaryData, JsonDocument>>()))
                .Returns(() =>
                {
                    if (callCount <= 0)
                    {
                        callCount++;
                        return JsonSerializer.Deserialize<JsonDocument>(File.ReadAllText(entityPath));
                    }

                    return null;
                });

            var source = CdmChangeFeedSource.Create(rootPath: "test",
                entityName: entityName,
                blobStorage: this.mockBlobStorageService.Object,
                changeCaptureInterval: TimeSpan.FromSeconds(1),
                schemaUpdateInterval: TimeSpan.FromSeconds(1));

            // Act
            await Source.FromGraph(source)
                .TakeWithin(TimeSpan.FromSeconds(5))
                .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

            // Assert
            this.mockBlobStorageService.Verify(s
                    => s.GetBlobContent("test", $"ChangeFeed/{entityName}.cdm.json",
                        It.IsAny<Func<BinaryData, JsonDocument>>()),
                Times.AtLeast(2)
            );
        }

        [Fact]
        public async Task ShouldFailIfNoSchemaPresentOnStart()
        {
            // Arrange
            const string entityName = "SchemaChangeTests";
            this.SetupTableMocks(entityName);
            this.mockBlobStorageService
                .Setup(mbs => mbs.GetBlobContent(It.IsAny<string>(),
                    $"ChangeFeed/{entityName}.cdm.json",
                    It.IsAny<Func<BinaryData, JsonDocument>>()))
                .Returns((string _, string _, Func<BinaryData, JsonDocument> callback) => callback(null));

            var source = CdmChangeFeedSource.Create(rootPath: "test",
                entityName: entityName,
                blobStorage: this.mockBlobStorageService.Object,
                changeCaptureInterval: TimeSpan.FromSeconds(1),
                schemaUpdateInterval: TimeSpan.FromSeconds(1));

            // Act
            var exception = await Assert.ThrowsAsync<SchemaNotFoundException>(async () =>
            {
                await Source.FromGraph(source)
                    .TakeWithin(TimeSpan.FromSeconds(5))
                    .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);
            });

            // Assert
            Assert.Equal("Could not found schema for entity: SchemaChangeTests", exception.Message);
        }

        private void SetupSchema(string entityName, string schemaUpdateName)
        {
            var callCount = 0;
            var entityPath = Path.Join(
                AppDomain.CurrentDomain.BaseDirectory,
                "Sources",
                "SampleData",
                "BaseEntity",
                entityName,
                $"{entityName}.cdm.json");
            var schemaPath = Path.Join(
                AppDomain.CurrentDomain.BaseDirectory,
                "Sources",
                "SampleData",
                "CdmChangeFeed",
                entityName,
                $"{entityName}.cdm.json");
            var updatedSchemaPath = Path.Join(
                AppDomain.CurrentDomain.BaseDirectory,
                "Sources",
                "SampleData",
                "CdmChangeFeed",
                entityName,
                $"{entityName}.{schemaUpdateName}.cdm.json");
            this.mockBlobStorageService
                .Setup(mbs => mbs.GetBlobContent(It.IsAny<string>(),
                    $"ChangeFeed/{entityName}.cdm.json",
                    It.IsAny<Func<BinaryData, JsonDocument>>()))
                .Returns(() =>
                {
                    switch (callCount)
                    {
                        case 0:
                            callCount++;
                            return JsonSerializer.Deserialize<JsonDocument>(File.ReadAllText(entityPath));
                        case 1:
                            callCount++;
                            return JsonSerializer.Deserialize<JsonDocument>(File.ReadAllText(schemaPath));
                        default:
                            return JsonSerializer.Deserialize<JsonDocument>(File.ReadAllText(updatedSchemaPath));
                    }
                });
        }

        [Theory]
        [InlineData(true, true, 11, "ValidEntity")]
        [InlineData(false, true, 19, "ValidEntity")]
        public async Task StopAfterBackfill(bool stopAfterBackfill, bool isBackfilling, int expectedRows, string entityName)
        {
            this.mockBlobStorageService.Reset();
            this.SetupTableMocks(entityName);
            var result = await Source.FromGraph(CdmChangeFeedSource.Create(rootPath: "test",
                    entityName: entityName,
                    blobStorage: this.mockBlobStorageService.Object,
                    isBackfilling: isBackfilling,
                    stopAfterBackfill: stopAfterBackfill,
                    changeCaptureInterval: TimeSpan.FromSeconds(1)))
                .TakeWithin(TimeSpan.FromSeconds(5))
                .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);
            this.mockBlobStorageService.Verify(
                mbs => mbs.ListBlobsAsEnumerable(It.IsAny<string>()),
                stopAfterBackfill ? Times.Once() : Times.AtLeast(2)
             );
            Assert.Equal(expectedRows, result.Count);
        }

        /// <summary>
        /// This test validates behavior of the lookBackInterval parameter.
        /// The mocked table last modified date is set to 1 hour ago.
        /// If lookBackInterval is set to 60 seconds, no rows should be
        /// returned since the last modified date is older than the lookBackInterval.
        /// </summary>
        /// <param name="lookBackInterval">Interval to test</param>
        /// <param name="entityName">Name of the entity to test</param>
        /// <param name="expectedRows">Expected number of rows</param>
        [Theory]
        [InlineData(60, "ValidEntity", 0)]
        [InlineData(3800, "ValidEntity", 8)]
        public async Task SupportsLockBackInterval(int lookBackInterval, string entityName, int expectedRows)
        {
            // Arrange
            this.mockBlobStorageService.Reset();
            this.SetupTableMocks(entityName, lastModified: DateTimeOffset.UtcNow.AddSeconds(-3600));

            // Act
            var result = await Source.FromGraph(CdmChangeFeedSource.Create(rootPath: "test",
                    entityName: entityName,
                    blobStorage: this.mockBlobStorageService.Object,
                    isBackfilling: false,
                    stopAfterBackfill: false,
                    lookBackRange: lookBackInterval,
                    changeCaptureInterval: TimeSpan.FromSeconds(1)))
                .TakeWithin(TimeSpan.FromSeconds(5))
                .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);


            // Assert
            Assert.Equal(expectedRows, result.Count);
        }

        [Theory]
        [InlineData(false, "InvalidBinaryType")]
        [InlineData(true, "InvalidBinaryType")]
        public async Task TestThrowsOnUnknownType(bool isBackfilling, string entityName)
        {
            this.SetupTableMocks("InvalidBinaryType");
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Source.FromGraph(
                    CdmChangeFeedSource.Create(rootPath: "test",
                        entityName: entityName,
                        blobStorage: this.mockBlobStorageService.Object,
                        isBackfilling: isBackfilling,
                        changeCaptureInterval: TimeSpan.FromSeconds(15)))
                .TakeWithin(TimeSpan.FromSeconds(5))
                .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer));
            Assert.Equal("Unknown primitive type: UnknownType", ex.Message);
        }

        [Fact]
        public void GetParquetSchema()
        {
            this.SetupTableMocks("ValidEntity");
            var schema = CdmChangeFeedSource.Create(rootPath: "test",
                entityName: "ValidEntity",
                blobStorage: this.mockBlobStorageService.Object,
                isBackfilling: true,
                changeCaptureInterval: TimeSpan.FromSeconds(15)).GetParquetSchema();

            Assert.Equal(Constants.UPSERT_MERGE_KEY, schema.Fields.Last().Name);
            Assert.Equal(typeof(string), (schema.Fields.Last() as DataField)!.ClrType);
        }

        [Fact]
        public void FailsIfChangeCaptureIntervalIsEmpty()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                var source = CdmChangeFeedSource.Create(rootPath: "test",
                    entityName: "ValidEntity",
                    blobStorage: this.mockBlobStorageService.Object,
                    isBackfilling: true,
                    changeCaptureInterval: TimeSpan.FromSeconds(0));
                Source.FromGraph(source)
                    .TakeWithin(TimeSpan.FromSeconds(5))
                    .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);
            });
        }

        private void SetupTableMocks(string entityName, DateTimeOffset? lastModified = null)
        {
            this.mockBlobStorageService.Setup(mbs => mbs.ListBlobsAsEnumerable(It.IsAny<string>())).Returns(
                new[]
                {
                    new StoredBlob
                    {
                        Name = $"Tables/Test1/Test2/{entityName.ToUpper()}_00001.csv",
                        LastModified = lastModified.GetValueOrDefault(DateTimeOffset.UtcNow),
                    }
                });

            this.mockBlobStorageService
                .Setup(mbs => mbs.ListBlobsAsEnumerable($"test/ChangeFeed/{entityName}"))
                .Returns(new[]
                {
                    new StoredBlob
                    {
                        Name = "ChangeFeed/changefeed_entry.csv",
                        LastModified = lastModified.GetValueOrDefault(DateTimeOffset.UtcNow),
                    }
                });

            var changeFeedSchema = JsonSerializer.Deserialize<JsonDocument>(File.ReadAllText(entityName.ToSampleCdmChangeFeedSchemaPath()));
            var tableSchema = JsonSerializer.Deserialize<JsonDocument>(File.ReadAllText(entityName.ToSampleCdmEntitySchemaPath()));

            this.mockBlobStorageService
                .Setup(mbs => mbs.GetBlobContent(It.IsAny<string>(),It.Is<string>(s => s.Contains("ChangeFeed")), It.IsAny<Func<BinaryData, JsonDocument>>()))
                .Returns(changeFeedSchema);

            this.mockBlobStorageService
                .Setup(mbs => mbs.GetBlobContent(It.IsAny<string>(),It.Is<string>(s => !s.Contains("ChangeFeed")),  It.IsAny<Func<BinaryData, JsonDocument>>()))
                .Returns(tableSchema);

            this.mockBlobStorageService
                .Setup(mbs =>
                    mbs.GetBlobContent($"test/ChangeFeed/{entityName}", "changefeed_entry.csv", It.IsAny<Func<BinaryData, string>>()))
                .Returns(ReadTestChangeFeedData(entityName));

            this.mockBlobStorageService
                .Setup(mbs =>
                    mbs.StreamBlobContent(It.IsAny<string>(), $"Test1/Test2/{entityName.ToUpper()}_00001.csv"))
                .Returns(() => ReadTestEntityData(entityName));
        }

        private static string ReadTestChangeFeedData(string entityName)
        {
            var path = Path.Join(AppDomain.CurrentDomain.BaseDirectory,
                "Sources",
                "SampleData",
                "CdmChangeFeed",
                entityName,
                "changefeed_entry.csv");
            return File.Exists(path) ? File.ReadAllText(path) : string.Empty;
        }

        private static Stream ReadTestEntityData(string entityName)
        {
            var path = Path.Join(AppDomain.CurrentDomain.BaseDirectory,
                        "Sources",
                        "SampleData",
                        "BaseEntity",
                        entityName,
                        $"{entityName.ToUpper()}_00001.csv");

            return File.Exists(path) ? new MemoryStream(Encoding.UTF8.GetBytes(File.ReadAllText(path))) : Stream.Null;
        }
    }
}
