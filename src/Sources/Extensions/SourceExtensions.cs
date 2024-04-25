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
/// Extensions for Source class
/// </summary>
public static class SourceExtensions
{
    /// <summary>
    /// Sink the source to Parquet sink
    /// </summary>
    /// <param name="source">The source to be sinked</param>
    /// <param name="parquetSchema">The Parquet schema</param>
    /// <param name="sinkLocation">The sink location</param>
    /// <param name="rowGroupsPerFile">The number of row groups per file</param>
    /// <param name="storageWriter">The storage writer</param>
    /// <param name="metricsService">The metrics service</param>
    /// <param name="dimensions">The dimensions</param>
    /// <param name="isBackFill">True if the source is back fill</param>
    /// <returns>The runnable graph</returns>
    public static IRunnableGraph<(UniqueKillSwitch, Task)> SinkToParquet(
        this Source<IEnumerable<List<DataCell>>, NotUsed> source,
        Schema parquetSchema,
        string sinkLocation,
        int rowGroupsPerFile,
        IBlobStorageWriter storageWriter,
        MetricsService metricsService,
        SortedDictionary<string, string> dimensions,
        bool isBackFill = false)
    {
        var parquetSink = ParquetSink.Create(
            parquetSchema,
            storageWriter,
            sinkLocation,
            rowGroupsPerFile,
            true,
            dataSinkPathSegment: isBackFill ? "backfill" : "data",
            dropCompletionToken: isBackFill);
        return source
            .Select(grp =>
            {
                var rows = grp.ToList();
                metricsService.Increment(DeclaredMetrics.ROWS_INCOMING, dimensions, rows.Count);
                return rows.AsRowGroup(parquetSchema);
            })
            .ViaMaterialized(KillSwitches.Single<List<DataColumn>>(), Keep.Right)
            .ToMaterialized(parquetSink, Keep.Both);
    }

    /// <summary>
    /// Converts Akka source to schema-free Arcane source
    /// </summary>
    /// <param name="source"></param>
    /// <typeparam name="TOut"></typeparam>
    /// <typeparam name="TMat"></typeparam>
    /// <returns></returns>
    public static ISchemaFreeSource<TOut, TMat> ToArcaneSource<TOut, TMat>(this Source<TOut, TMat> source)
        => new SchemaFreeSource<TOut, TMat>(source);
}
