using System;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Base;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// Wraps a source that does not require a schema
/// </summary>
/// <typeparam name="TOut">Output element type</typeparam>
/// <typeparam name="TMat">Type of the materialized value of the source</typeparam>
public interface ISchemaFreeSource<TOut, TMat> : IArcaneSource<TOut, TMat>
{
    /// <summary>
    /// Applies a transformation to the output of the source
    /// </summary>
    /// <param name="mapper">Mapper function</param>
    /// <typeparam name="TOut2">Type of the new output</typeparam>
    /// <returns>Source that emits a new value</returns>
    ISchemaFreeSource<TOut2, TMat> Map<TOut2>(Func<TOut, TOut2> mapper);

    /// <summary>
    /// Maps the source to a new source
    /// </summary>
    /// <param name="mapper">Mapper function</param>
    /// <typeparam name="TOut2">Type of the new output</typeparam>
    /// <returns>Source that emits a new value</returns>
    ISchemaFreeSource<TOut2, TMat> MapSource<TOut2>(Func<Source<TOut, TMat>, Source<TOut2, TMat>> mapper);


    /// <summary>
    /// Connects the source to a flow
    /// </summary>
    /// <param name="flow">Flow to connect to</param>
    /// <typeparam name="TOut2">Flow output type</typeparam>
    /// <returns>A new source created by connecting the source to the flow wrapped with ISchemaFreeSource</returns>
    ISchemaFreeSource<TOut2, TMat> Via<TOut2>(IGraph<FlowShape<TOut, TOut2>, TMat> flow);

    /// <summary>
    /// Connects the source to a sink
    /// </summary>
    /// <param name="sink">Sink to connect to</param>
    /// <typeparam name="TMat2">Type of Sink's materialized value</typeparam>
    /// <returns>Runnable graph</returns>
    IRunnableGraph<TMat2> To<TMat2>(ISchemaFreeSink<TOut, TMat2> sink);
}
