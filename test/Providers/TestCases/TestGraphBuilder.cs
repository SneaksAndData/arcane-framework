using System;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Services.Base;

namespace Arcane.Framework.Tests.Providers.TestCases;

public class TestGraphBuilder : IStreamGraphBuilder<TestStreamContext>, IStreamGraphBuilder<IStreamContext>
{
    public IRunnableGraph<(UniqueKillSwitch, Task)> BuildGraph(TestStreamContext context)
    {
        return Source.Single(1)
            .ViaMaterialized(KillSwitches.Single<int>(), Keep.Right)
            .ToMaterialized(Sink.Ignore<int>(), Keep.Both)
            .MapMaterializedValue(tuple => (tuple.Item1, (Task)tuple.Item2));
    }

    public IRunnableGraph<(UniqueKillSwitch, Task)> BuildGraph(IStreamContext context)
    {
        return Source.Single(1)
            .ViaMaterialized(KillSwitches.Single<int>(), Keep.Right)
            .ToMaterialized(Sink.Ignore<int>(), Keep.Both)
            .MapMaterializedValue(tuple => (tuple.Item1, (Task)tuple.Item2));
    }
}

public class TestFailedGraphBuilder : IStreamGraphBuilder<TestStreamContext>, IStreamGraphBuilder<IStreamContext>
{
    private readonly Exception exception;

    public TestFailedGraphBuilder(Exception exception)
    {
        this.exception = exception;
    }

    public IRunnableGraph<(UniqueKillSwitch, Task)> BuildGraph(TestStreamContext context)
    {
        throw this.exception;
    }

    public IRunnableGraph<(UniqueKillSwitch, Task)> BuildGraph(IStreamContext context)
    {
        throw this.exception;
    }
}

