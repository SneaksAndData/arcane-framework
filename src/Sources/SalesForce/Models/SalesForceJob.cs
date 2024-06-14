using System.Text.Json.Serialization;

namespace Arcane.Framework.Sources.SalesForce.Models;

/// <summary>
/// Job status types
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SalesforceJobStatus
{
    UploadComplete,
    InProgress,
    Aborted,
    JobComplete,
    Failed,
    None
}

/// <summary>
/// Represents Salesforce job
/// </summary>
public class SalesForceJob
{
    /// <summary>
    /// Id
    /// </summary>
    [JsonPropertyName("id")]
    public string Id { get; set; }


    /// <summary>
    /// Job status
    /// </summary>
    [JsonPropertyName("state")]
    public SalesforceJobStatus Status { get; set; }

    /// <summary>
    /// object
    /// </summary>
    [JsonPropertyName("object")]
    public string Object { get; set; }

    /// <summary>
    /// Total processing time of the job
    /// </summary>
    [JsonPropertyName("totalProcessingTime")]
    public long? TotalProcessingTime { get; set; }

    /// <summary>
    /// Numbers of records processed by the job
    /// </summary>
    [JsonPropertyName("numberRecordsProcessed")]
    public long? NumberRecordsProcessed { get; set; }

}
