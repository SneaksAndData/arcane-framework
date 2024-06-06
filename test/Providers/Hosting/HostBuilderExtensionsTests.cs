using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Framework.Contracts;
using Arcane.Framework.Providers.Hosting;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Sources.Exceptions;
using Arcane.Framework.Tests.Providers.TestCases;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Xunit;

namespace Arcane.Framework.Tests.Providers.Hosting;

public class HostBuilderExtensionsTests
{
    private readonly Mock<IStreamStatusService> streamStatusServiceMock = new();

    [Fact]
    public async Task ShouldHandleTypedHostedService()
    {
        // Arrange
        var host = new HostBuilder().ConfigureRequiredServices(
                services =>
                    services.AddStreamGraphBuilder<TestGraphBuilder, TestStreamContext>(
                        () => new TestStreamContext(),
                        _ => this.streamStatusServiceMock.Object),
                getStreamHostContextBuilder: CreateContext)
            .Build();

        // Act
        var exitCode = await host.RunStream<TestStreamContext>(Mock.Of<Serilog.ILogger>());

        // Assert
        Assert.Equal(ExitCodes.SUCCESS, exitCode);
    }

    [Fact]
    public async Task ShouldHandleUntypedHostedService()
    {
        // Arrange
        var host = new HostBuilder()
            .ConfigureRequiredServices(services => services.AddStreamGraphBuilder<TestGraphBuilder>(
                    _ => new TestStreamContext(),
                    getStreamStatusService: _ => this.streamStatusServiceMock.Object,
                    getStreamHostContextBuilder: CreateContext),
                getStreamHostContextBuilder: CreateContext)
            .Build();

        // Act
        var exitCode = await host.RunStream(Mock.Of<Serilog.ILogger>());

        // Assert
        Assert.Equal(ExitCodes.SUCCESS, exitCode);
    }

    [Theory]
    [MemberData(nameof(GenerateExceptionTestCases))]
    public async Task TestArcaneExceptionHandler(Exception exception, int expectedExitCode)
    {
        // Arrange
        var host = new HostBuilder()
            .ConfigureRequiredServices(services => services.AddStreamGraphBuilder<TestFailedGraphBuilder>(
                    _ => new TestStreamContext(),
                    getStreamStatusService: _ => this.streamStatusServiceMock.Object,
                    getStreamHostContextBuilder: CreateContext),
                getStreamHostContextBuilder: CreateContext)
            .ConfigureServices(s => s.AddSingleton(exception))
            .Build();

        // Act
        var exitCode = await host.RunStream(Mock.Of<Serilog.ILogger>(),
            (ex, _) => ex is DivideByZeroException
                ? Task.FromResult(35.AsOption())
                : Task.FromResult(Option<int>.None));

        // Assert
        Assert.Equal(expectedExitCode, exitCode);
    }


    public static IEnumerable<object[]> GenerateExceptionTestCases()
    {
        yield return new object[] { new SchemaInconsistentException(1, 2), ExitCodes.RESTART };
        yield return new object[] { new SchemaMismatchException(), ExitCodes.SUCCESS };
        yield return new object[] { new Exception(), ExitCodes.FATAL };
        yield return new object[] { new DivideByZeroException(), 35 };
    }

    private static StreamingHostBuilderContext CreateContext()
    {
        return new StreamingHostBuilderContext
        {
            IsBackfilling = false,
            StreamId = "StreamId",
            StreamKind = "StreamKind"
        };
    }
}
