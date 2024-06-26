﻿using Arcane.Framework.Services.Base;

namespace Arcane.Framework.Tests.Providers.TestCases;

public class TestStreamContext : IStreamContext, IStreamContextWriter
{
    public string StreamId => nameof(StreamId);
    public bool IsBackfilling => false;
    public string StreamKind => nameof(StreamKind);
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
