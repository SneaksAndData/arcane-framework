using System;
using Akka.Streams.Stage;
using Arcane.Framework.Sources.Base;

namespace Arcane.Framework.Sources.Extensions;

/// <summary>
/// Extensions for GraphStageLogic
/// </summary>
public static class GraphStageLogicExtensions
{
    /// <summary>
    /// Complete stage if StopAfterFullLoad is set to true and source finished full load mode
    /// </summary>
    /// <param name="logic">The source logic</param>
    /// <param name="cleanup">Additional cleanup action to be invoked before complete the stage</param>
    /// <returns>True if stage has been completed</returns>
    public static bool CompleteStageAfterFullLoad<TSource>(this TSource logic, Action<Exception> cleanup = null)
        where TSource : GraphStageLogic, IStopAfterBackfill
    {
        if (logic.IsRunningInBackfillMode)
        {
            if (logic.StopAfterBackfill)
            {
                cleanup?.Invoke(null);
                logic.CompleteStage();
                return true;
            }

            logic.IsRunningInBackfillMode = false;
        }

        return false;
    }
}
