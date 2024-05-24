using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using Arcane.Framework.Sources.RestApi.Services.PageResolvers;
using Xunit;

namespace Arcane.Framework.Tests.Sources;

public class PageResolverTests
{
    [Fact]
    public void TestCounterResponse()
    {
        var resolver = new PageOffsetResolver(3, new[] { "data" }, 0);
        foreach (var (message, continuePagination) in RestApiServiceResponseSequence())
        {

            Assert.Equal(resolver.Next(message), continuePagination);
        }
    }

    private static IEnumerable<(HttpResponseMessage, bool)> RestApiServiceResponseSequence()
    {
        var emptyResponseContent = new Dictionary<string, object[]> { {"data", Array.Empty<object>() } };
        var emptyMessage =  new HttpResponseMessage
        {
            Content = new StringContent(JsonSerializer.Serialize(emptyResponseContent))
        };

        var filledContent = new Dictionary<string, object[]>
        {
            {"data", new[] { new object(), new object(), new object() } }
        };
        var filledMessage = new HttpResponseMessage
        {
            Content = new StringContent(JsonSerializer.Serialize(filledContent))
        };

        yield return (emptyMessage, true);
        yield return (filledMessage, true);
        yield return (emptyMessage,  false);
        yield return (filledMessage, true);
    }

}
