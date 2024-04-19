using System;
using Akka.Streams.Dsl;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// Wraps a sink that does not require a schema
/// </summary>
/// <typeparam name="TIn">Input element tye</typeparam>
/// <typeparam name="TMat">Type of materialized value</typeparam>
public interface ISchemaFreeSink<TIn, TMat>
{
    /// <summary>
    /// Returns a graph builder function that connects a source to the sink
    /// </summary>
    IRunnableGraph<TMat> GraphBuilder<TMat2>(Source<TIn, TMat2> source);

}
