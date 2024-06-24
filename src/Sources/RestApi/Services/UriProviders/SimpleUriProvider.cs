using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Akka.Util;
using Arcane.Framework.Sources.RestApi.Models;
using Arcane.Framework.Sources.RestApi.Services.UriProviders.Base;

namespace Arcane.Framework.Sources.RestApi.Services.UriProviders;

/// <summary>
/// The SimpleUriProvider class that implements the IRestApiUriProvider interface.
/// </summary>
public class SimpleUriProvider : IRestApiUriProvider
{
    private readonly List<RestApiTemplatedField> templatedFields;
    private readonly DateTimeOffset backFillStartDate;
    private readonly RestApiTemplate bodyTemplate;
    private readonly HttpMethod requestMethod;
    private readonly RestApiTemplate urlTemplate;

    /// <summary>
    /// The SimpleUriProvider class that implements the IRestApiUriProvider interface.
    /// </summary>
    /// <param name="urlTemplate">URL template</param>
    /// <param name="templatedFields">URL template fields</param>
    /// <param name="backFillStartDate">Start date for running in the backfill mode</param>
    /// <param name="requestMethod">HTTP method to invoke</param>
    /// <param name="bodyTemplate">HTTP body template</param>
    public SimpleUriProvider(string urlTemplate, List<RestApiTemplatedField> templatedFields,
        DateTimeOffset backFillStartDate, HttpMethod requestMethod, string bodyTemplate = null)
    {
        this.urlTemplate = new RestApiTemplate(urlTemplate, templatedFields.Select(v => v.FieldName).ToList());
        this.bodyTemplate = !string.IsNullOrEmpty(bodyTemplate)
            ? new RestApiTemplate(bodyTemplate, templatedFields.Select(v => v.FieldName).ToList())
            : RestApiTemplate.Empty();
        this.BaseUri = new Uri(urlTemplate);
        this.templatedFields = templatedFields;
        this.backFillStartDate = backFillStartDate;
        this.requestMethod = requestMethod;
    }

    /// <inheritdoc cref="IRestApiUriProvider.BaseUri"/>
    public Uri BaseUri { get; }

    /// <inheritdoc cref="IRestApiUriProvider.GetNextResultUri"/>
    public (Option<Uri> nextUri, HttpMethod requestMethod, Option<string> payload) GetNextResultUri(
        Option<HttpResponseMessage> paginatedResponse,
        bool isBackfill,
        TimeSpan lookBackInterval,
        TimeSpan changeCaptureInterval)
    {
        var resultUri = this.urlTemplate.CreateResolver();
        var resultBody = this.bodyTemplate.CreateResolver();


        if (isBackfill && !paginatedResponse.IsEmpty)
        {
            return (Option<Uri>.None, this.requestMethod, Option<string>.None);
        }

        var filterTimestamp = (isFullLoad: isBackfill, paginatedResponse.IsEmpty) switch
        {
            (true, _) => this.backFillStartDate,
            (false, true) => DateTimeOffset.UtcNow.Subtract(lookBackInterval),
            (false, false) => DateTimeOffset.UtcNow.Subtract(changeCaptureInterval)
        };

        if (this.templatedFields.FirstOrDefault(field =>
                field.FieldType is TemplatedFieldType.FILTER_DATE_FROM or TemplatedFieldType.FILTER_DATE_BETWEEN_FROM)
            is { } dateField)
        {
            switch (dateField.Placement)
            {
                case TemplatedFieldPlacement.URL:
                    resultUri = resultUri.ResolveField(dateField.FieldName,
                        filterTimestamp.ToString(dateField.FormatString));
                    break;
                case TemplatedFieldPlacement.BODY:
                    resultBody = resultBody.ResolveField(dateField.FieldName,
                        filterTimestamp.ToString(dateField.FormatString));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        if (this.templatedFields.FirstOrDefault(
                field => field.FieldType == TemplatedFieldType.FILTER_DATE_BETWEEN_TO) is { } dateFieldTo)
        {
            switch (dateFieldTo.Placement)
            {
                case TemplatedFieldPlacement.URL:
                    resultUri = resultUri.ResolveField(dateFieldTo.FieldName,
                        DateTimeOffset.UtcNow.ToString(dateFieldTo.FormatString));
                    break;
                case TemplatedFieldPlacement.BODY:
                    resultBody = resultBody.ResolveField(dateFieldTo.FieldName,
                        DateTimeOffset.UtcNow.ToString(dateFieldTo.FormatString));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        return (new Uri(resultUri.GetResolvedRequestElement()), this.requestMethod,
            resultBody.GetResolvedRequestElement());
    }
}
