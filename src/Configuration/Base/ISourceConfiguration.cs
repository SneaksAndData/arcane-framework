namespace Arcane.Stream.RestApi.Models;

public interface ISourceConfiguration
{
    /// <summary>
    /// Data location for parquet files.
    /// </summary>
    public string SinkLocation { get; init; }
}
