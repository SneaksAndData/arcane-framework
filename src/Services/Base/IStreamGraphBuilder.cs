using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace Arcane.Framework.Services.Base;

/// <summary>
/// Base interface for classes that build Akka.NET stream graphs.
/// This interface is intended to be implemented by the streaming plugin.
/// </summary>
/// <typeparam name="TStreamContext">Type of the StreamContext class</typeparam>
public interface IStreamGraphBuilder<in TStreamContext> where TStreamContext : IStreamContext
{
    /// <summary>
    /// Build an instance of the runnable Akka.NET graph.
    /// </summary>
    /// <param name="context">StreamContext instance.</param>
    /// <returns>Runnable graph that materialized to a UniqueKillSwitch and a Task.</returns>
    public IRunnableGraph<(UniqueKillSwitch, Task)> BuildGraph(TStreamContext context);
}
