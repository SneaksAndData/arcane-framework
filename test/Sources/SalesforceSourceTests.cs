using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.SalesForce;
using Arcane.Framework.Sources.SalesForce.Services.AuthenticatedMessageProviders;
using Arcane.Framework.Tests.Fixtures;
using Moq;
using Xunit;

namespace Arcane.Framework.Tests.Sources;

public class SalesforceSourceTests : IClassFixture<AkkaFixture>
{
    private readonly AkkaFixture akkaFixture;

    private readonly SalesForceJobProvider jobProvider;
    private readonly Mock<HttpClient> mockHttp;


    public SalesforceSourceTests(AkkaFixture akkaFixture)
    {
        this.akkaFixture = akkaFixture;
        this.mockHttp = new Mock<HttpClient>();
        this.jobProvider = new SalesForceJobProvider("test.my.salesforce.com", "client_id", "client_secret", "user_name", "password", "security_token", "v60.0", 5);

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


        var token = await this.jobProvider.GetAuthenticatedMessage(this.mockHttp.Object);
        Assert.Equal("<long_token>", token.Headers.Authorization.Parameter);
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
                        msg.RequestUri == new Uri("https://test.my.salesforce.com/services/data/v60.0/jobs/query/750QI000007QKdRYAW/results?maxRecords=5&")),
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
                        msg.RequestUri == new Uri("https://test.my.salesforce.com/services/data/v60.0/jobs/query/750QI000007QKdRYAW/results?maxRecords=5&locator=abcdefg")),
                    It.IsAny<CancellationToken>()))
            .ReturnsAsync(mockGetResultResponse2);

        var source = SalesForceSource.Create(this.jobProvider, this.mockHttp.Object, "account", TimeSpan.FromSeconds(5));

        var result = await Source.FromGraph(source)
            .Take(10)
            .RunWith(Sink.Seq<List<DataCell>>(), this.akkaFixture.Materializer);

        Assert.Equal(10, result.Count);
    }

}
