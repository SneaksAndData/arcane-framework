using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Arcane.Framework.Configuration;

/// <summary>
/// Converts Seconds to/from TimeSpan for StreamContext properties serialization/deserialization
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public class SecondsToTimeSpanConverter: JsonConverter<TimeSpan>
{
    /// <inheritdoc cref="JsonConverter{T}.Read"/>>
    public override TimeSpan Read( ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        TimeSpan.FromSeconds(reader.GetInt64());

    /// <inheritdoc cref="JsonConverter{T}.Write"/>>
    public override void Write(Utf8JsonWriter writer, TimeSpan timeSpanValue, JsonSerializerOptions options) =>
        writer.WriteNumberValue(timeSpanValue.TotalSeconds);
}
