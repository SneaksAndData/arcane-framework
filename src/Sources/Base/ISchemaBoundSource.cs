using System;
using Akka.Streams.Dsl;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// Wraps a source that requires a schema
/// </summary>
/// <typeparam name="TOut">Output element type</typeparam>
/// <typeparam name="TMat">Type of materialized value</typeparam>
/// <typeparam name="TSchema">Schema type</typeparam>
public interface ISchemaBoundSource<TOut, TMat, TSchema> : ISchemaFreeSource<TOut, TMat> where TSchema : ISchemaValidator<TOut>
{
    /// <summary>
    /// Converts the source's materialized value to a new value and attaches a new schema to it
    /// </summary>
    /// <param name="mapper">Value mapper</param>
    /// <param name="newSchema">New data schema</param>
    /// <typeparam name="TOut2">Type of new value</typeparam>
    /// <typeparam name="TNewSchema">Type of a new schema</typeparam>
    /// <returns>A source with a new schema attached</returns>
    ISchemaBoundSource<TOut2, TMat, TNewSchema> Map<TOut2, TNewSchema>(Func<TOut, TOut2> mapper, TNewSchema newSchema) where TNewSchema : ISchemaValidator<TOut2>;

    /// <summary>
    /// Connects the source to a sink
    /// </summary>
    /// <param name="sink">Sink to connect to</param>
    /// <typeparam name="TMat2">Materialized value of the sink</typeparam>
    /// <returns>Runnable graph</returns>
    IRunnableGraph<TMat2> To<TMat2>(ISchemaBoundSink<TOut, TMat2, TSchema> sink);

    /// <summary>
    /// Returns the schema validator
    /// </summary>
    TSchema Schema { get; }
}
