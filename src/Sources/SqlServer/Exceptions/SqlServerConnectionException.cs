using System;
using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Sources.SqlServer.Exceptions;

/// <summary>
/// Exception thrown when a connection to SQL Server could not be established.
/// </summary>
[ExcludeFromCodeCoverage]
public class SqlServerConnectionException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SqlServerConnectionException"/> class.
    /// </summary>
    /// <param name="streamId">Stream identifier</param>
    /// <param name="innerException">Inner exception</param>
    public SqlServerConnectionException(string streamId, Exception innerException) :
        base($"Failed to establish connection with SQL Server for stream {streamId}", innerException)
    {
    }
}
