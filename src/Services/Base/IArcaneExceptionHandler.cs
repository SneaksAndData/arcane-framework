using System;
using System.Threading.Tasks;
using Akka.Util;

namespace Arcane.Framework.Services.Base;

/// <summary>
/// Common interface for built-in exception handling in the Arcane framework.
/// This interface is not intended to be implemented by the streaming plugin.
/// </summary>
internal interface IArcaneExceptionHandler
{
    /// <summary>
    /// Handle an exception and return an application exit code. If the exception is not known, return None.
    /// </summary>
    /// <param name="exception">Exception to handle</param>
    /// <returns>Optional application exit code</returns>
    Task<Option<int>> HandleException(Exception exception);
}
