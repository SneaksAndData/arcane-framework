using Akka.Streams.Dsl;
using Arcane.Framework.Sources.Base;

namespace Arcane.Framework.Sources.Extensions;

/// <summary>
/// Extensions for Source class
/// </summary>
public static class SourceExtensions
{
    /// <summary>
    /// Converts Akka source to schema-free Arcane source
    /// </summary>
    /// <param name="source"></param>
    /// <typeparam name="TOut"></typeparam>
    /// <typeparam name="TMat"></typeparam>
    /// <returns></returns>
    public static ISchemaFreeSource<TOut, TMat> ToArcaneSource<TOut, TMat>(this Source<TOut, TMat> source)
        => new SchemaFreeSource<TOut, TMat>(source);
}
