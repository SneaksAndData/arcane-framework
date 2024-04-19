using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Metrics.Models;
using Arcane.Framework.Sinks;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.Base;
using Parquet.Data;
using Snd.Sdk.Metrics.Base;
using Snd.Sdk.Storage.Base;

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
