using System;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Base;
using Arcane.Framework.Sources.Base;
using Arcane.Framework.Sources.Extensions;

namespace Arcane.Framework.Sources;

/// <inheritdoc />
public class SchemaBoundSource<TOut, TMat, TSchema> : ISchemaBoundSource<TOut, TMat, TSchema>
    where TSchema : ISchemaValidator<TOut>
{
    private readonly ISchemaFreeSource<TOut, TMat> source;

    /// <summary>
    /// Creates a new schema-bound source
    /// </summary>
    /// <param name="source">Underlying schema-free source</param>
    /// <param name="schema">Data schema</param>
    public SchemaBoundSource(ISchemaFreeSource<TOut, TMat> source, TSchema schema)
    {
        this.source = source;
        this.Schema = schema;
    }

    /// <inheritdoc />
    public TSchema Schema { get; }

    /// <inheritdoc />
    public IArcaneSource<TOut, TMat2> MapMaterializedValue<TMat2>(Func<TMat, TMat2> mapper)
    {
        return this.source.MapMaterializedValue(mapper);
    }


    /// <inheritdoc />
    public ISchemaFreeSource<TOut2, TMat> Map<TOut2>(Func<TOut, TOut2> mapper)
    {
        return this.source.Map(mapper);
    }


    /// <inheritdoc />
    public ISchemaFreeSource<TOut2, TMat> MapSource<TOut2>(Func<Source<TOut, TMat>, Source<TOut2, TMat>> mapper)
    {
        return this.source.MapSource(mapper);
    }

    /// <inheritdoc />
    public IRunnableGraph<TMat2> To<TMat2>(ISchemaFreeSink<TOut, TMat2> sink)
    {
        return this.source.To(sink);
    }

    /// <inheritdoc />
    public ISchemaFreeSource<TOut2, TMat> Via<TOut2>(IGraph<FlowShape<TOut, TOut2>, TMat> flow)
    {
        return this.source.Via(flow);
    }

    /// <inheritdoc />
    public ISchemaBoundSource<TOut2, TMat, TNewSchema> Map<TOut2, TNewSchema>(Func<TOut, TOut2> mapper,
        TNewSchema newSchema) where TNewSchema : ISchemaValidator<TOut2>
    {
        return this.source.Map(mapper).WithSchema(newSchema);
    }

    /// <inheritdoc />
    public IRunnableGraph<TMat2> To<TMat2>(ISchemaBoundSink<TOut, TMat2, TSchema> sink)
    {
        return sink.GraphBuilder(this);
    }
}
