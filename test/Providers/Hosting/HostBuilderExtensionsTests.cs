using System.Threading.Tasks;
using Arcane.Framework.Providers.Hosting;
using Arcane.Framework.Tests.Providers.TestCases;
using Microsoft.Extensions.Hosting;
using Moq;
using Xunit;

namespace Arcane.Framework.Tests.Providers.Hosting;


public class HostBuilderExtensionsTests
{
    [Fact]
    public async Task ShouldHandleTypedHostedService()
    {
        // Arrange
        var host = new HostBuilder().ConfigureRequiredServices(
                services =>
                    services.AddStreamGraphBuilder<TestGraphBuilder, TestStreamContext>(
                        contextProvider: () => new TestStreamContext()),
                contextBuilder: CreateContext)
            .Build();

        // Act
        var exitCode = await host.RunStream<TestStreamContext>(Mock.Of<Serilog.ILogger>());

        // Assert
        Assert.Equal(0, exitCode);
    }

    [Fact]
    public async Task ShouldHandleUntypedHostedService()
    {
        // Arrange
        var host = new HostBuilder()
            .ConfigureRequiredServices(
                services => services.AddStreamGraphBuilder<TestGraphBuilder>(_ => new TestStreamContext(),
                    contextBuilder: CreateContext), contextBuilder: CreateContext).Build();

        // Act
        var exitCode = await host.RunStream(Mock.Of<Serilog.ILogger>());

        // Assert
        Assert.Equal(0, exitCode);
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
