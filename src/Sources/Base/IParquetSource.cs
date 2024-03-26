using Parquet.Data;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// Implemented by sources that support ParquetSink."/>
/// </summary>
public interface IParquetSource
{
    /// <summary>
    /// Apache Parquet schema for the elements emitted by this source.
    /// </summary>
    /// <returns></returns>
    Schema GetParquetSchema();
}
