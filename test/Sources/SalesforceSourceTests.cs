using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.SalesForce;
using Arcane.Framework.Sources.SalesForce.Models;
using Arcane.Framework.Sources.SalesForce.Services.AuthenticatedMessageProviders;
using Arcane.Framework.Tests.Fixtures;
using Microsoft.OpenApi.Models;
using Moq;
using Polly;
using Snd.Sdk.Tasks;
using Xunit;

namespace Arcane.Framework.Tests.Sources;

public class SalesforceSourceTests : IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;
    // private readonly SimpleUriProvider db;
    private readonly DynamicBearerAuthenticatedMessageProvider dynamicAuth;
    private readonly Mock<HttpClient> mockHttp;
    // private readonly PagedUriProvider pdb;

    public SalesforceSourceTests(AkkaFixture akkaFixture)
    {
        this.akkaFixture = akkaFixture;
        this.mockHttp = new Mock<HttpClient>();
        // this.db = new SimpleUriProvider("https://localhost/data?date=@date", new List<RestApiTemplatedField>
        // {
        //     new()
        //     {
        //         FieldName = "date", FieldType = TemplatedFieldType.FILTER_DATE_FROM,
        //         FormatString = "yyyy-MM-ddTHH:mm:ssZ", Placement = TemplatedFieldPlacement.URL
        //     }
        // }, new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero), HttpMethod.Get);
        // this.fa = new FixedHeaderAuthenticatedMessageProvider(new Dictionary<string, string> { { "Bearer", "test" } });
        // this.dynamicAuth =
        //     new DynamicBearerAuthenticatedMessageProvider("https://localhost/auth", "token", "expiresIn");
        // this.pdb = new PagedUriProvider(
        //         "https://localhost/data_paged?page=@page&filter=updatedAt>=<@dateFrom,@dateTo",
        //         new List<RestApiTemplatedField>
        //         {
        //             new()
        //             {
        //                 FieldName = "page", FieldType = TemplatedFieldType.RESPONSE_PAGE, FormatString = string.Empty,
        //                 Placement = TemplatedFieldPlacement.URL
        //             },
        //             new()
        //             {
        //                 FieldName = "dateFrom", FieldType = TemplatedFieldType.FILTER_DATE_BETWEEN_FROM,
        //                 FormatString = "yyyyMMddHHmmss", Placement = TemplatedFieldPlacement.URL
        //             },
        //             new()
        //             {
        //                 FieldName = "dateTo", FieldType = TemplatedFieldType.FILTER_DATE_BETWEEN_TO,
        //                 FormatString = "yyyyMMddHHmmss", Placement = TemplatedFieldPlacement.URL
        //             }
        //         },
        //         new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero),
        //         HttpMethod.Get)
        //     .WithPageResolver(new PageResolverConfiguration
        //     { ResolverPropertyKeyChain = new[] { "TotalPages" }, ResolverType = PageResolverType.COUNTER });
    }

    [Fact]
    public async Task TokenGeneration()
    {
        var mockContent = new
        {
            access_token = "<long_token>",
            instance_url = "https://test.my.salesforce.com",
            id = "https://test.salesforce.com/id/00D3E000000D2OCUA0/0057Y0000068CtmQAE",
            token_type = "Bearer",
            issued_at = "1718179384536",
            signature = "abc"
        };

        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(msg =>
                        msg.RequestUri == new Uri("https://test.my.salesforce.com/services/oauth2/token")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent(JsonSerializer.Serialize(mockContent)) });


        var auth = new DynamicBearerAuthenticatedMessageProvider("test.my.salesforce.com", "client_id", "client_secret", "user_name", "password", "security_token");

        var token = await auth.GetAuthenticatedMessage(this.mockHttp.Object);
        Assert.Equal("<long_token>", token.Headers.Authorization.Parameter);
    }



    [Fact]
    public async Task JobDeserialization()
    {


        var job = JsonSerializer.Deserialize<SalesForceJob>(@"{
    ""id"": ""750QI000007MuMUYA0"",
    ""operation"": ""query"",
    ""object"": ""ECCO_Consent__c"",
    ""createdById"": ""0057Y0000068CtmQAE"",
    ""createdDate"": ""2024-06-11T08:30:10.000+0000"",
    ""systemModstamp"": ""2024-06-11T08:30:49.000+0000"",
    ""state"": ""JobComplete"",
    ""concurrencyMode"": ""Parallel"",
    ""contentType"": ""CSV"",
    ""apiVersion"": 60.0,
    ""jobType"": ""V2Query"",
    ""lineEnding"": ""LF"",
    ""columnDelimiter"": ""COMMA"",
    ""numberRecordsProcessed"": 449464,
    ""retries"": 0,
    ""totalProcessingTime"": 63674,
    ""isPkChunkingSupported"": true
}");

    }

    [Fact]
    public async void RunStream()
    {

        var mockTokenContent = new
        {
            access_token = "<long_token>",
            instance_url = "https://test.my.salesforce.com",
            id = "https://test.salesforce.com/id/00D3E000000D2OCUA0/0057Y0000068CtmQAE",
            token_type = "Bearer",
            issued_at = "1718179384536",
            signature = "abc"
        };

        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(msg =>
                        msg.RequestUri == new Uri("https://test.my.salesforce.com/services/oauth2/token")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent(JsonSerializer.Serialize(mockTokenContent)) });

        var mockCreateJobContent = new
        {
            id = "750QI000007QKdRYAW",
            operation = "query",
            @object = "ECCO_Consent__c",
            createdById = "0057Y0000068CtmQAE",
            createdDate = "2024-06-13T06:57:56.000+0000",
            systemModstamp = "2024-06-13T06:57:56.000+0000",
            state = "UploadComplete",
            concurrencyMode = "Parallel",
            contentType = "CSV",
            apiVersion = 60.0,
            lineEnding = "LF",
            columnDelimiter = "COMMA"
        };

        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(msg =>
                        msg.RequestUri == new Uri("https://test.my.salesforce.com/services/data/v60.0/jobs/query") &&
                        msg.Method == HttpMethod.Post),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent(JsonSerializer.Serialize(mockCreateJobContent)) });

        var mockGetSchemaContent = new
        {
            totalSize = 2,
            done = true,
            records = new[]{
                new {

                    Name = "Id",
                    @DataType = "id",
                    ValueTypeId = "id"
                },
                new {

                    Name = "Name",
                    @DataType = "string",
                    ValueTypeId = "string"
                }
            }
        };

        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(msg =>
                        msg.RequestUri.ToString().Contains("https://test.my.salesforce.com/services/data/v60.0/query?q=")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent(JsonSerializer.Serialize(mockGetSchemaContent)) });



        var mockUpdateJobStatusContent = new
        {
            id = "750QI000007QKdRYAW",
            operation = "query",
            @object = "ECCO_Consent__c",
            createdById = "0057Y0000068CtmQAE",
            createdDate = "2024-06-12T08:03:19.000+0000",
            systemModstamp = "2024-06-12T08:03:41.000+0000",
            state = "JobComplete",
            concurrencyMode = "Parallel",
            contentType = "CSV",
            apiVersion = 60.0,
            jobType = "V2Query",
            lineEnding = "LF",
            columnDelimiter = "COMMA",
            numberRecordsProcessed = 449465,
            retries = 0,
            totalProcessingTime = 72011,
            isPkChunkingSupported = true
        };

        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(msg =>
                        msg.RequestUri == new Uri("https://test.my.salesforce.com/services/data/v60.0/jobs/query/750QI000007QKdRYAW")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent(JsonSerializer.Serialize(mockUpdateJobStatusContent)) });

        var mockGetResultContent = @"""Id"",""Name""
""1a"",""aa""
""2b"",""bb""
""3c"",""cc""
""4d"",""dd""
""5e"",""ee""";
        var mockGetResultResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(mockGetResultContent)
        };
        mockGetResultResponse.Headers.Add("Sforce-Locator", "abcdefg");


        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(msg =>
                        msg.RequestUri == new Uri("https://test.my.salesforce.com/services/data/v60.0/jobs/query/750QI000007QKdRYAW/results?maxRecords=50&")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(mockGetResultResponse);

        var mockGetResultContent2 = @"""Id"",""Name""
""6f"",""ff""
""7g"",""gg""
""8h"",""hh""
""9i"",""ii""
""10j"",""jj""";
        var mockGetResultResponse2 = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(mockGetResultContent2)
        };
        mockGetResultResponse2.Headers.Add("Sforce-Locator", "null");


        this.mockHttp
            .Setup(http =>
                http.SendAsync(
                    It.Is<HttpRequestMessage>(msg =>
                        msg.RequestUri == new Uri("https://test.my.salesforce.com/services/data/v60.0/jobs/query/750QI000007QKdRYAW/results?maxRecords=50&locator=abcdefg")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(mockGetResultResponse2);

        var auth = new DynamicBearerAuthenticatedMessageProvider("test.my.salesforce.com", "client_id", "client_secret", "user_name", "password", "security_token");

        var source = SalesForceSource.Create(auth, this.mockHttp.Object, "account", TimeSpan.FromSeconds(5));

        var result = await Source.FromGraph(source)
            .Take(10)
            .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

        Assert.Equal(10, result.Count);
    }

    private class MockResult
    {
        public string MockValue { get; set; }
    }
}
