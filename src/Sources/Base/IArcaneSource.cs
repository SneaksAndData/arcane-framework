using System;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// Wraps a source that does not require a schema
/// </summary>
/// <typeparam name="TOut">Output element type</typeparam>
/// <typeparam name="TMat">Type of materialized value</typeparam>
public interface IArcaneSource<out TOut, out TMat>
{
    /// <summary>
    /// Converts the source's materialized value to a new value
    /// </summary>
    /// <param name="mapper">Mapper function</param>
    /// <typeparam name="TMat2">Type of new materialized value</typeparam>
    /// <returns>Source with new materialized value</returns>
    IArcaneSource<TOut, TMat2> MapMaterializedValue<TMat2>(Func<TMat, TMat2> mapper);
}
