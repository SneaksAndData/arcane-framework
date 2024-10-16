using Akka.Util;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Sinks.Models;

namespace Arcane.Framework.Tests.Providers.TestCases;

public class TestStreamContext : IStreamContext, IStreamContextWriter
{
    public string StreamId => nameof(StreamId);
    public bool IsBackfilling => false;
    public string StreamKind => nameof(StreamKind);
    public Option<StreamMetadata> GetStreamMetadata() => new();

    public void SetStreamId(string streamId)
    {
        /* do nothing */
    }

    public void SetBackfilling(bool isRunningInBackfillMode)
    {
        /* do nothing */
    }

    public void SetStreamKind(string streamKind)
    {
        /* do nothing */
    }
}
