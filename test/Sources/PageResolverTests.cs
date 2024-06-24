using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using Akka.Util;
using Arcane.Framework.Sources.RestApi.Services.PageResolvers;
using Xunit;

namespace Arcane.Framework.Tests.Sources;

public class PageResolverTests
{
    [Fact]
    public void TestCounterPageResolver()
    {
        var resolver = new PageOffsetResolver(3, new[] { "data" }, 0);
        foreach (var (message, continuePagination) in RestApiArrayResponseSequence())
        {

            Assert.Equal( continuePagination, resolver.Next(message));
        }
    }

    [Fact]
    public void TestTokenPageResolver()
    {
        var resolver = new PageNextTokenResolver(new[] { "next" });
        foreach (var (message, continuePagination) in RestApiTokenResponseSequence())
        {

            Assert.Equal(continuePagination, resolver.Next(message));
        }
    }

    private static IEnumerable<(Option<HttpResponseMessage>, bool)> RestApiArrayResponseSequence()
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

        yield return (emptyMessage,  true);
        yield return (filledMessage, true);
        yield return (filledMessage, true);
        yield return (emptyMessage,  false);
    }

    private static IEnumerable<(Option<HttpResponseMessage>, bool)> RestApiTokenResponseSequence()
    {
        var emptyResponseContent = new Dictionary<string, object> { {"next", "next-1" } };
        var emptyMessage =  new HttpResponseMessage
        {
            Content = new StringContent(JsonSerializer.Serialize(emptyResponseContent))
        };

        var filledContent = new Dictionary<string, object>
        {
            {"next", "http://example.com/next_page" }
        };
        var filledMessage = new HttpResponseMessage
        {
            Content = new StringContent(JsonSerializer.Serialize(filledContent))
        };

        yield return (Option<HttpResponseMessage>.None, true);
        yield return (filledMessage, false);
        yield return (filledMessage, false);
        yield return (emptyMessage,  false);
    }
}
