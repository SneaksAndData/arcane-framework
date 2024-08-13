using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Arcane.Framework.Sources.RestApi;
using Arcane.Framework.Sources.RestApi.Models;
using Arcane.Framework.Sources.RestApi.Services.AuthenticatedMessageProviders;
using Arcane.Framework.Sources.RestApi.Services.UriProviders;
using Arcane.Framework.Tests.Fixtures;
using Microsoft.OpenApi.Models;
using Moq;
using Polly;
using Polly.RateLimit;
using Xunit;

namespace Arcane.Framework.Tests.Sources;

public class RestApiSourceTests : IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    private readonly SimpleUriProvider db;
    private readonly DynamicBearerAuthenticatedMessageProvider dynamicAuth;
    private readonly FixedHeaderAuthenticatedMessageProvider fa;
    private readonly Mock<HttpClient> mockHttp;
    private readonly PagedUriProvider pdb;

    public RestApiSourceTests(AkkaFixture akkaFixture)
    {
        this.akkaFixture = akkaFixture;
        this.mockHttp = new Mock<HttpClient>();
        this.db = new SimpleUriProvider("https://localhost/data?date=@date", new List<RestApiTemplatedField>
        {
            new()
            {
                FieldName = "date", FieldType = TemplatedFieldType.FILTER_DATE_FROM,
                FormatString = "yyyy-MM-ddTHH:mm:ssZ", Placement = TemplatedFieldPlacement.URL
            }
        }, new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero), HttpMethod.Get);
        this.fa = new FixedHeaderAuthenticatedMessageProvider(new Dictionary<string, string> { { "Bearer", "test" } });
        this.dynamicAuth =
            new DynamicBearerAuthenticatedMessageProvider("https://localhost/auth", "token", "expiresIn");
        this.pdb = new PagedUriProvider(
                "https://localhost/data_paged?page=@page&filter=updatedAt>=<@dateFrom,@dateTo",
                new List<RestApiTemplatedField>
                {
                    new()
                    {
                        FieldName = "page", FieldType = TemplatedFieldType.RESPONSE_PAGE, FormatString = string.Empty,
                        Placement = TemplatedFieldPlacement.URL
                    },
                    new()
                    {
                        FieldName = "dateFrom", FieldType = TemplatedFieldType.FILTER_DATE_BETWEEN_FROM,
                        FormatString = "yyyyMMddHHmmss", Placement = TemplatedFieldPlacement.URL
                    },
                    new()
                    {
                        FieldName = "dateTo", FieldType = TemplatedFieldType.FILTER_DATE_BETWEEN_TO,
                        FormatString = "yyyyMMddHHmmss", Placement = TemplatedFieldPlacement.URL
                    }
                },
                new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero),
                HttpMethod.Get)
            .WithPageResolver(new PageResolverConfiguration
                { ResolverPropertyKeyChain = ["TotalPages"], ResolverType = PageResolverType.COUNTER });
    }

    [Fact]
    public async Task RestApiSourceCanBackFill()
    {
        var mockContent = new[]
            { new MockResult { MockValue = "val1" }, new MockResult { MockValue = "val2" } };

        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(msg =>
                        msg.RequestUri == new Uri("https://localhost/data?date=2023-01-01T00:00:00Z")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                { Content = new StringContent(JsonSerializer.Serialize(mockContent)) });

        var src = RestApiSource.Create(
            this.db,
            this.fa,
            true,
            TimeSpan.FromSeconds(90),
            TimeSpan.FromDays(30),
            this.mockHttp.Object,
            true,
            Policy.RateLimitAsync(1, TimeSpan.FromSeconds(60)),
            new OpenApiSchema());

        var result = await Source.FromGraph(src).RunWith(Sink.Seq<JsonElement>(), this.akkaFixture.Materializer);

        Assert.Equal(JsonSerializer.Serialize(mockContent), JsonSerializer.Serialize(result));
    }

    /// <summary>
    /// This is difficult to test with rate limit lower than test time due to how Polly Policy kicks in
    /// In order to reduce the total time to test this, a very low rate limit is used, so we only expect 2 elements to come out
    /// before the policy takes effect
    /// </summary>
    [Fact]
    public async Task RestApiSourceStreamAndRateLimit()
    {
        var mockContent = new[]
            { new MockResult { MockValue = "val1" }, new MockResult { MockValue = "val2" } };

        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.IsAny<HttpRequestMessage>(),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                { Content = new StringContent(JsonSerializer.Serialize(mockContent)) });

        var src = RestApiSource.Create(
            this.db,
            this.fa,
            false,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromDays(30),
            this.mockHttp.Object,
            false,
            Policy.RateLimitAsync(1, TimeSpan.FromSeconds(60)),
            new OpenApiSchema());

        var result = await Source.FromGraph(src).TakeWithin(TimeSpan.FromSeconds(5))
            .RunWith(Sink.Seq<JsonElement>(), this.akkaFixture.Materializer);

        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task RestApiPaged()
    {
        var mockContent = new MockChainResultRoot
        {
            TotalPages = 3,
            MockRootValue = new MockChainResult
            {
                MockEntryValue = new[]
                {
                    new MockResult { MockValue = "val1" },
                    new MockResult { MockValue = "val2" }
                }
            }
        };

        // first invocation
        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(hrm =>
                        hrm.RequestUri.ToString().Contains("page=1") &&
                        hrm.RequestUri.ToString().Contains("updatedAt>=<")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                { Content = new StringContent(JsonSerializer.Serialize(mockContent)) });

        // following invocations, page must be set
        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(hrm =>
                        (hrm.RequestUri.ToString().Contains("page=2") ||
                         hrm.RequestUri.ToString().Contains("page=3")) &&
                        hrm.RequestUri.ToString().Contains("updatedAt>=<")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                { Content = new StringContent(JsonSerializer.Serialize(mockContent)) });

        // over-the-last invocation, should return empty object
        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(hrm =>
                        hrm.RequestUri.ToString().Contains("page=4") &&
                        hrm.RequestUri.ToString().Contains("updatedAt>=<")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
                { Content = new StringContent("{}") });

        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(hrm => hrm.RequestUri.ToString().Contains("auth")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(JsonSerializer.Serialize(new
                {
                    token = "test",
                    expiresIn = 3600
                }))
            });

        var src = RestApiSource.Create(this.pdb, this.dynamicAuth, true, TimeSpan.FromSeconds(5), TimeSpan.FromDays(1),
            this.mockHttp.Object, true, Policy.RateLimitAsync(1, TimeSpan.FromSeconds(5)),
            new OpenApiSchema(),
            ["MockRootValue", "MockEntryValue"]);

        var result = await Source.FromGraph(src).RunWith(Sink.Seq<JsonElement>(), this.akkaFixture.Materializer);

        Assert.Equal(mockContent.TotalPages * 2, result.Count);
    }

    [Fact]
    public void FailsIfChangeCaptureIntervalIsEmpty()
    {
        Assert.Throws<ArgumentException>(() =>
        {
            var source = RestApiSource.Create(
                this.pdb,
                this.dynamicAuth,
                true,
                TimeSpan.FromSeconds(0),
                TimeSpan.FromDays(1),
                this.mockHttp.Object,
                true,
                Policy.RateLimitAsync(1, TimeSpan.FromSeconds(5)),
                new OpenApiSchema(),
                ["MockRootValue", "MockEntryValue"]);
            Source.FromGraph(source)
                .TakeWithin(TimeSpan.FromSeconds(5))
                .RunWith(Sink.Seq<JsonElement>(), this.akkaFixture.Materializer);
        });
    }

    private class MockResult
    {
        public string MockValue { get; set; }
    }

    private class MockChainResult
    {
        public MockResult[] MockEntryValue { get; set; }
    }

    private class MockChainResultRoot
    {
        public int TotalPages { get; set; }
        public MockChainResult MockRootValue { get; set; }
    }
}
