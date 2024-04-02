using System;

namespace Arcane.Framework.Sinks.Parquet.Models;

/// <summary>
/// Represents a single value in a Parquet Column
/// </summary>
public sealed class DataCell
{
    /// <summary>
    /// Create a DataCell instance
    /// </summary>
    /// <param name="fieldName">Field name</param>
    /// <param name="fieldType">CLR type of the field</param>
    /// <param name="value">Field value</param>
    public DataCell(string fieldName, Type fieldType, object value)
    {
        FieldName = fieldName;
        FieldType = fieldType;
        Value = value;
    }

    /// <summary>
    /// Field name
    /// </summary>
    public string FieldName { get; }

    /// <summary>
    /// CLR type of the field
    /// </summary>
    public Type FieldType { get; }

    /// <summary>
    /// Field value
    /// </summary>
    public object Value { get; }
}
