﻿using System;
using Akka.Streams.Dsl;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// Wraps a sink that requires a schema
/// </summary>
/// <typeparam name="TIn">Input element type</typeparam>
/// <typeparam name="TMat">Type of materialized value</typeparam>
/// <typeparam name="TSchema">Type of the schema validator</typeparam>
public interface ISchemaBoundSink<TIn, TMat, TSchema>  where TSchema : ISchemaValidator<TIn>
{
    /// <summary>
    /// Returns a graph builder function that connects a source to the sink
    /// </summary>
    IRunnableGraph<TMat> GraphBuilder<TMat2>(ISchemaBoundSource<TIn, TMat2, TSchema> source);

    /// <summary>
    /// Returns the schema validator
    /// </summary>
    TSchema Schema { get; }
}
