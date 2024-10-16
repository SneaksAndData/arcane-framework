using Arcane.Framework.Sinks.Models;
using Arcane.Framework.Sinks.Services;
using Arcane.Framework.Sinks.Services.Base;
using Arcane.Framework.Sinks.Services.StreamMetadata.V1;
using Snd.Sdk.Storage.Base;

namespace Arcane.Framework.Sinks.Extensions;

/// <summary>
/// Extension methods for StreamMetadata class
/// </summary>
public static class StreamMetadataExtensions
{
    /// <summary>
    /// Creates a new IMetadataWriter instance from the StreamMetadata instance
    /// </summary>
    /// <param name="streamMetadata">The object that hods the stream metadata</param>
    /// <param name="writer">Blob Storage writer to be used to perform the write operation.</param>
    /// <param name="basePath">The Sink root directory</param>
    /// <returns>
    /// Metadata writer instance that can write one or more metadata fields to the given root directory.
    /// </returns>
   public static IMetadataWriter ToStreamMetadataWriter(this StreamMetadata streamMetadata,
       IBlobStorageWriter writer,
       string basePath)
   {
       return new PartitionsWriter(streamMetadata.Partitions, writer, basePath);
   }
}
