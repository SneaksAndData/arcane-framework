using Arcane.Framework.Sources.Base;

namespace Arcane.Framework.Sources.Extensions;

/// <summary>
/// Extension methods for a ISchemaFreeSource interface
/// </summary>
public static class SchemaFreeSourceExtensions
{
    /// <summary>
    /// Converts a schema-free source to a schema bound source
    /// </summary>
    /// <param name="source">Source to convert</param>
    /// <param name="schema">Data schema</param>
    /// <typeparam name="TOut">Output element type</typeparam>
    /// <typeparam name="TMat">Type of materialized value</typeparam>
    /// <typeparam name="TSchema">Type of the schema validator</typeparam>
    /// <returns>Schema bound source</returns>
    public static ISchemaBoundSource<TOut, TMat, TSchema> WithSchema<TOut, TMat, TSchema>(
        this ISchemaFreeSource<TOut, TMat> source,
        TSchema schema) where TSchema : ISchemaValidator<TOut>
    {
        return new SchemaBoundSource<TOut, TMat, TSchema>(source,
            schema);
    }
}
