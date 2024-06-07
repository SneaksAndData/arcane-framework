using System.Diagnostics.CodeAnalysis;

namespace Arcane.Framework.Contracts;

/// <summary>
/// Common container exit codes
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Trivial")]
public static class ExitCodes
{
    /// <summary>
    /// Job completed successfully
    /// </summary>
    public const int SUCCESS = 0;

    /// <summary>
    /// Fatal error occurred, Kubernetes Job controller must increment the retry counter
    /// </summary>
    public const int FATAL = 1;

    /// <summary>
    /// Fatal error occurred, Kubernetes Job controller must NOT increment the retry counter and restart the job
    /// </summary>
    public const int RESTART = 2;
}
