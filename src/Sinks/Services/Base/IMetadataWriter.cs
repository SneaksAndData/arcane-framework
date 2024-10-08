using System.Threading.Tasks;
using Akka.Event;

namespace Arcane.Framework.Sinks.Services.Base;

/// <summary>
/// Writes stream metadata to a blob storage
/// </summary>
public interface IMetadataWriter
{
    /// <summary>
    /// Writes stream metadata to a blob storage
    /// </summary>
    /// <param name="loggingAdapter">Logging adapter provider by the Sink stage</param>
    /// <returns>Task that completes when write operation is completed</returns>
    Task Write(ILoggingAdapter loggingAdapter);
}
