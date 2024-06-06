using System;
using Akka;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Base;
using Arcane.Framework.Sources;
using Arcane.Framework.Sources.Base;

namespace Arcane.Framework.Sinks;

/// <inheritdoc />
public class SchemaBoundSink<TIn, TMat, TSchema> : ISchemaBoundSink<TIn, TMat, TSchema>
    where TSchema : ISchemaValidator<TIn>
{
    private readonly ISchemaFreeSink<TIn, TMat> sink;

    /// <summary>
    /// Creates a new schema-bound sink
    /// </summary>
    /// <param name="sink">Underlying Akka sink</param>
    /// <param name="schema">Data schema</param>
    public SchemaBoundSink(ISchemaFreeSink<TIn, TMat> sink, TSchema schema)
    {
        this.Schema = schema;
        this.sink = sink;
    }


    /// <inheritdoc />
    public IRunnableGraph<TMat> GraphBuilder<TMat2>(ISchemaBoundSource<TIn, TMat2, TSchema> source) =>
        source.Via(this.Schema.Validate<TMat2>()).To(this.sink);

    /// <inheritdoc />
    public TSchema Schema { get; }
}
