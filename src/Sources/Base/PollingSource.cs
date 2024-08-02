using System;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Arcane.Framework.Sources.Base;

/// <summary>
/// Base class for source logic classes for polling sources
/// </summary>
public abstract class PollingSourceLogic: TimerGraphStageLogic
{
    /// <summary>
    /// Creates a new instance of <see cref="PollingSourceLogic"/>
    /// </summary>
    /// <param name="changeCaptureInterval">Interval for pulling changes in the data source. Should be greater then zero</param>
    /// <param name="shape">Stage shape</param>
    /// <exception cref="ArgumentException">Thrown if changeCaptureInterval is less or equal to zero</exception>
    protected PollingSourceLogic(TimeSpan changeCaptureInterval, Shape shape) : base(shape)
    {
        if (changeCaptureInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException("Change capture interval must be greater than zero.");
        }
        ChangeCaptureInterval = changeCaptureInterval;
    }

    /// <summary>
    /// Interval for pulling changes in the data source
    /// </summary>
    protected TimeSpan ChangeCaptureInterval { get; private init; }
}
