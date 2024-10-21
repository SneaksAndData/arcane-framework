namespace Arcane.Framework.Sinks.Parquet.Models;

/// <summary>
/// Constant values used by sources and sinks.
/// </summary>
public static class Constants
{
    /// <summary>
    /// Name for a merge key column in the parquet schema file metadata.
    /// </summary>
    public const string UPSERT_MERGE_KEY_NAME = "ARCANE_MERGE_KEY_NAME";

    /// <summary>
    /// Name for a merge key column attached to sources that support upserts.
    /// </summary>
    public const string UPSERT_MERGE_KEY = "ARCANE_MERGE_KEY";

    /// <summary>
    /// Name for a partition key column base on a datetime expression.
    /// </summary>
    public const string DATE_PARTITION_KEY = "DATE_PARTITION_KEY";
}
