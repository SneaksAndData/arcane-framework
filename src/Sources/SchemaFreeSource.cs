using System;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Base;
using Arcane.Framework.Sources.Base;

namespace Arcane.Framework.Sources;

/// <inheritdoc />
public class SchemaFreeSource<TOut, TMat> : ISchemaFreeSource<TOut, TMat>
{
    private readonly Source<TOut, TMat> source;

    /// <summary>
    /// Creates a new instance from underlying source
    /// </summary>
    /// <param name="source">Akka source to wrap</param>
    public SchemaFreeSource(Source<TOut, TMat> source)
    {
        this.source = source;
    }

    /// <inheritdoc />
    public ISchemaFreeSource<TOut2, TMat> Map<TOut2>(Func<TOut, TOut2> mapper)
    {
        return new SchemaFreeSource<TOut2, TMat>(this.source.Via(Flow.FromFunction(mapper)));
    }

    /// <inheritdoc />
    public IArcaneSource<TOut, TMat2> MapMaterializedValue<TMat2>(Func<TMat, TMat2> mapper)
    {
        return new SchemaFreeSource<TOut, TMat2>(this.source.MapMaterializedValue(mapper));
    }

    /// <inheritdoc />
    public ISchemaFreeSource<TOut2, TMat> MapSource<TOut2>(Func<Source<TOut, TMat>, Source<TOut2, TMat>> mapper)
    {
        return new SchemaFreeSource<TOut2, TMat>(mapper(this.source));
    }

    /// <inheritdoc />
    public IRunnableGraph<TMat2> To<TMat2>(ISchemaFreeSink<TOut, TMat2> sink)
    {
        return sink.GraphBuilder(this.source);
    }

    /// <inheritdoc />
    public ISchemaFreeSource<TOut2, TMat> Via<TOut2>(IGraph<FlowShape<TOut, TOut2>, TMat> flow)
    {
        return new SchemaFreeSource<TOut2, TMat>(this.source.Via(flow));
    }
}
