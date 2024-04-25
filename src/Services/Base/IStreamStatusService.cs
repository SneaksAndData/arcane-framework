using System.Threading.Tasks;

namespace Arcane.Framework.Services.Base;

/// <summary>
/// A service that allows to report stream status events to the maintainer service.
/// It can be useful to report a stream status that triggers the maintainer service to take action (schema mismatch, etc.)
/// </summary>
public interface IStreamStatusService
{
    /// <summary>
    /// Report that schema mismatch occured to the maintainer service.
    /// </summary>
    /// <param name="streamId">Id of the current stream.</param>
    Task ReportSchemaMismatch(string streamId);
}

