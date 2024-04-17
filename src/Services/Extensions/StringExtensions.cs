using System;

namespace Arcane.Framework.Services.Extensions;

/// <summary>
/// Extension methods for string objects
/// </summary>
public static class StringExtensions
{
    private static string DefaultVarPrefix => $"{nameof(Arcane).ToUpper()}__";

    /// <summary>
    /// Returns the environment variable name
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public static string GetEnvironmentVariableName(this string name) => $"{DefaultVarPrefix}{name}".ToUpperInvariant();

    /// <summary>
    /// Returns the environment variable value
    /// </summary>
    /// <param name="name">Environment variable name</param>
    /// <returns>Value as a string</returns>
    /// <exception cref="ArgumentNullException">Thrown if the variable is not present in environment</exception>
    public static string GetEnvironmentVariable(this string name)
        => Environment.GetEnvironmentVariable(GetEnvironmentVariableName(name)) ??
           throw new ArgumentNullException(name);
}
