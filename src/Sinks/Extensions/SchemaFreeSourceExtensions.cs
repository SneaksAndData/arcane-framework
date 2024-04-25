using Arcane.Framework.Sources.Base;

namespace Arcane.Framework.Sinks.Extensions;

/// <summary>
/// Extension methods for a ISchemaFreeSource interface
/// </summary>
public static class SchemaFreeSinkExtensions
{
    /// <summary>
    /// Converts a schema-free sink to a schema bound sink
    /// </summary>
    /// <param name="sink">sink to convert</param>
    /// <param name="schema">Data schema</param>
    /// <typeparam name="TOut">Output element type</typeparam>
    /// <typeparam name="TMat">Type of materialized value</typeparam>
    /// <typeparam name="TSchema">Type of the schema validator</typeparam>
    /// <returns>Schema bound sink</returns>
    public static ISchemaBoundSink<TOut, TMat, TSchema>
        WithSchema<TOut, TMat, TSchema>(this ISchemaFreeSink<TOut, TMat> sink, TSchema schema)
        where TSchema : ISchemaValidator<TOut> => new SchemaBoundSink<TOut, TMat, TSchema>(sink, schema);
}
