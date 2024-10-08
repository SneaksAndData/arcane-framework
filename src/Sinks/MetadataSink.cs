using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Stage;
using Akka.Util;
using Arcane.Framework.Sinks.Models;
using Snd.Sdk.Storage.Base;

namespace Arcane.Framework.Sinks;

public interface IMetadataWriter
{
    Task WriteMetadata(StreamMetadata metadata);
}

public class PartitionsWriter: IMetadataWriter
{
    private readonly IBlobStorageReader blobStorateReader;
    private readonly IBlobStorageWriter blobStorateWriter;
    private readonly Option<StreamPartition[]> partitions;
    private const string MetadataFileName = "metadata/partitions.json";

    public PartitionsWriter(Option<StreamPartition[]> partition, IBlobStorageReader reader, IBlobStorageWriter writer)
    {
        this.blobStorateReader = reader;
        this.blobStorateWriter = writer;
        this.partitions = partition;
    }

    public Task WriteMetadata(StreamMetadata metadata, string basePath)
    {
        if (!this.partitions.HasValue)
        {
            this.blobStorateWriter.RemoveBlob($"{basePath}/{MetadataFileName}");
        }
    }
}

public abstract class MetadataLogic : GraphStageLogic
{
    private readonly StreamMetadata streamMetadata;

    public override void PreStart()
    {
        throw new System.NotImplementedException();
    }

    protected MetadataLogic(StreamMetadata metadata, Shape shape) : base(shape)
    {
        this.streamMetadata = metadata;
    }
}
