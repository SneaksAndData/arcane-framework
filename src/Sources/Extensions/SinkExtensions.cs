using Akka.Streams.Dsl;
using Arcane.Framework.Sinks;
using Arcane.Framework.Sinks.Base;
using Arcane.Framework.Sources.Base;

namespace Arcane.Framework.Sources.Extensions;

/// <summary>
/// Extensions for the Sink class
/// </summary>
public static class SinkExtensions
{
    /// <summary>
    /// Converts Akka sink to schema-free Arcane sink
    /// </summary>
    /// <param name="sink"></param>
    /// <typeparam name="TIn"></typeparam>
    /// <typeparam name="TMat"></typeparam>
    /// <returns></returns>
    public static ISchemaFreeSink<TIn, TMat> ToArcaneSink<TIn, TMat>(this Sink<TIn, TMat> sink) => new SchemaFreeSink<TIn, TMat>(sink);
}
