using System;

namespace Arcane.Stream.RestApi.Models;

public interface ISinkConfiguration
{
    /// <summary>
    /// Number of JsonElements per single json file.
    /// </summary>
    public int RowsPerGroup { get; init; }

    /// <summary>
    /// Max time to wait for rowsPerGroup to accumulate.
    /// </summary>
    public TimeSpan GroupingInterval { get; init; }
    
    /// <summary>
    /// Data location for parquet files.
    /// </summary>
    public string SinkLocation { get; init; }
}
