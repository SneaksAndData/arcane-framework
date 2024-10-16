using System.Text.Json;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Util;
using Arcane.Framework.Sinks.Models;
using Arcane.Framework.Sinks.Services.Base;
using Snd.Sdk.Storage.Base;

namespace Arcane.Framework.Sinks.Services.StreamMetadata.V0;

/// <summary>
/// A stream metadata writer that writes partitions metadata to a blob storage
/// </summary>
public class PartitionsWriter: IMetadataWriter
{
    private readonly IBlobStorageWriter blobStorageWriter;
    private readonly Option<StreamPartition[]> partitions;
    private readonly string basePath;
    private const string MetadataFileName = "v0/partitions.json";

    /// <summary>
    /// A stream metadata writer that writes partitions metadata to a blob storage
    /// </summary>
    /// <param name="partitions">Partitioning info for consumers</param>
    /// <param name="writer">The blob storage writer instance</param>
    /// <param name="basePath">Path to the stream metadata directory</param>
    public PartitionsWriter(Option<StreamPartition[]> partitions,
        IBlobStorageWriter writer,
        string basePath)
    {
        this.blobStorageWriter = writer;
        this.partitions = partitions;
        this.basePath = basePath;
    }

    /// <inheritdoc cref="IMetadataWriter.Write"/>
    public Task Write(ILoggingAdapter loggingAdapter)
    {
        if (!this.partitions.HasValue)
        {
            loggingAdapter.Info("No partitions to write, removing existing metadata file if exists");
            return this.blobStorageWriter.RemoveBlob(this.basePath, MetadataFileName);
        }
        loggingAdapter.Info("Writing partitions metadata to {basePath}", this.basePath);
        var partitionsJson = JsonSerializer.Serialize(this.partitions.Value);
        return this.blobStorageWriter.SaveTextAsBlob(partitionsJson, this.basePath, MetadataFileName);
    }
}
