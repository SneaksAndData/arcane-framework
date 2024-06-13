using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Arcane.Framework.Sources.SalesForce.Models;


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
/// Represents CDM Change Feed entity
/// </summary>
public class SalesForceJob
{
    /// <summary>
    /// Id
    /// </summary>
    [JsonPropertyName("id")]
    public string Id { get; set; }


    /// <summary>
    /// Attributes collection
    /// </summary>
    [JsonPropertyName("state")]
    public SalesforceJobStatus Status { get; set; }

    [JsonPropertyName("object")]
    public string Object { get; set; }


    [JsonPropertyName("totalProcessingTime")]
    public long? TotalProcessingTime { get; set; }

    [JsonPropertyName("numberRecordsProcessed")]
    public long? NumberRecordsProcessed { get; set; }

}
