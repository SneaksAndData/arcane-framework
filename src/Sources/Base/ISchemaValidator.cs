using Akka.Streams.Dsl;
using Arcane.Framework.Sources.Exceptions;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// Interface for data schema that can validate input data during stream processing
/// </summary>
/// <typeparam name="TIn">Input data type</typeparam>
public interface ISchemaValidator<TIn>
{
    /// <summary>
    /// Returns a flow that validates data against a schema and throws one of the
    /// <see cref="SchemaException"/> inherited exceptions if validation failed
    /// </summary>
    /// <typeparam name="TMat">Type of materialized value</typeparam>
    /// <returns>Akka stream flow</returns>
    Flow<TIn, TIn, TMat> Validate<TMat>();
}
