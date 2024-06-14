using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Data.SqlClient;

namespace Arcane.Framework.Sources.Extensions;

/// <summary>
/// Utilities for SQL Server database operations.
/// </summary>
internal static class SqlServerUtils
{
    /// <summary>
    /// Get all columns for the specified table.
    /// </summary>
    /// <param name="schema">Schema name</param>
    /// <param name="table">Table name</param>
    /// <param name="sqlConnection">SQL connection to use</param>
    /// <returns>Column name and True if it is a primary key</returns>
    public static IEnumerable<(string columnName, bool isPrimaryKey)> GetColumns(string schema, string table,
        SqlConnection sqlConnection)
    {
        var getColumnsSql = File
            .ReadAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Sources", "SqlServer", "SqlSnippets",
                "GetColumns.sql"))
            .Replace("{dbName}", sqlConnection.Database)
            .Replace("{schema}", schema)
            .Replace("{table}", table);

        var command = new SqlCommand(getColumnsSql, sqlConnection);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            yield return (reader.GetString(0), reader.GetInt32(1) == 1);
        }
    }
}
