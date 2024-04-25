using System;
using Akka.Streams.Dsl;
using Arcane.Framework.Sources.Base;

namespace Arcane.Framework.Sinks;

/// <inheritdoc />
public class SchemaFreeSink<TIn, TMat>: ISchemaFreeSink<TIn, TMat>
{
    private readonly Sink<TIn,TMat> sink;

    /// <summary>
    /// Creates a new schema-free sink
    /// </summary>
    /// <param name="sink">Underlying Akka sink</param>
    public SchemaFreeSink(Sink<TIn, TMat> sink)
    {
        this.sink = sink;
    }

    /// <inheritdoc />
    public IRunnableGraph<TMat> GraphBuilder<TMat2>(Source<TIn, TMat2> source)
    {
        return source.ToMaterialized(this.sink, Keep.Right);
    }
}
