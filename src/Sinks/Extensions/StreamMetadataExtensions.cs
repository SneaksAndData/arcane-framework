using Akka.Event;
using Arcane.Framework.Sinks.Models;
using Arcane.Framework.Sinks.Services;
using Arcane.Framework.Sinks.Services.Base;
using Snd.Sdk.Storage.Base;

namespace Arcane.Framework.Sinks.Extensions;

public static class StreamMetadataExtensions
{
   public static IMetadataWriter ToStreamMetadataWriter(this StreamMetadata streamMetadata,
       IBlobStorageWriter writer,
       string basePath)
   {
       return new PartitionsWriter(streamMetadata.Partitions, writer, basePath);
   }
}
