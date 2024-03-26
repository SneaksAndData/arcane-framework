using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Akka.Util;
using Arcane.Framework.Sources.RestApi.Models;
using Arcane.Framework.Sources.RestApi.Services.PageResolvers;
using Arcane.Framework.Sources.RestApi.Services.PageResolvers.Base;
using Arcane.Framework.Sources.RestApi.Services.UriProviders.Base;

namespace Arcane.Framework.Sources.RestApi.Services.UriProviders;

/// <summary>
/// The URI provider for paginated APIs.
/// </summary>
public class PagedUriProvider : IPaginatedApiUriProvider
{
    private readonly DateTimeOffset _backFillStartDate;
    private readonly RestApiTemplate bodyTemplate;
    private readonly HttpMethod requestMethod;
    private readonly List<RestApiTemplatedField> templatedFields;
    private readonly RestApiTemplate urlTemplate;

    private IPageResolver pageResolver;
    private TimestampRange queryRange;


    /// <summary>
    /// The SimpleUriProvider class that implements the IRestApiUriProvider interface.
    /// </summary>
    /// <param name="urlTemplate">URL template</param>
    /// <param name="templatedFields">URL template fields</param>
    /// <param name="backFillStartDate">Start date for running in the backfill mode</param>
    /// <param name="requestMethod">HTTP method to invoke</param>
    /// <param name="bodyTemplate">HTTP body template</param>
    public PagedUriProvider(
        string urlTemplate,
        List<RestApiTemplatedField> templatedFields,
        DateTimeOffset backFillStartDate,
        HttpMethod requestMethod,
        string bodyTemplate = null)
    {
        BaseUri = new Uri(urlTemplate);
        this._backFillStartDate = backFillStartDate;
        this.requestMethod = requestMethod;
        this.templatedFields = templatedFields;
        this.urlTemplate = new RestApiTemplate(urlTemplate,
            templatedFields.Where(field => field.Placement == TemplatedFieldPlacement.URL).Select(v => v.FieldName)
                .ToList());
        this.bodyTemplate = !string.IsNullOrEmpty(bodyTemplate)
            ? new RestApiTemplate(bodyTemplate,
                templatedFields.Where(field => field.Placement == TemplatedFieldPlacement.BODY).Select(v => v.FieldName)
                    .ToList())
            : RestApiTemplate.Empty();
    }

    /// <inheritdoc cref="IRestApiUriProvider.GetNextResultUri"/>
    public (Option<Uri> nextUri, HttpMethod requestMethod, Option<string> payload) GetNextResultUri(
        Option<HttpResponseMessage> paginatedResponse,
        bool isBackfill,
        TimeSpan lookBackInterval,
        TimeSpan changeCaptureInterval)
    {
        var resultUri = urlTemplate.CreateResolver();
        var resultBody = bodyTemplate.CreateResolver();

        queryRange ??= isBackfill
            ? new TimestampRange(_backFillStartDate, DateTimeOffset.UtcNow)
            : paginatedResponse.IsEmpty
                ? new TimestampRange(DateTimeOffset.UtcNow.Subtract(lookBackInterval), DateTimeOffset.UtcNow)
                : new TimestampRange(DateTimeOffset.UtcNow.Subtract(changeCaptureInterval), DateTimeOffset.UtcNow);

        if (templatedFields.FirstOrDefault(field =>
                field.FieldType is TemplatedFieldType.FILTER_DATE_FROM or TemplatedFieldType.FILTER_DATE_BETWEEN_FROM)
            is { } dateField)
            switch (dateField.Placement)
            {
                case TemplatedFieldPlacement.URL:
                    resultUri = resultUri.ResolveField(dateField.FieldName,
                        queryRange.Start.ToString(dateField.FormatString));
                    break;
                case TemplatedFieldPlacement.BODY:
                    resultBody = resultBody.ResolveField(dateField.FieldName,
                        queryRange.Start.ToString(dateField.FormatString));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

        if (templatedFields.FirstOrDefault(field => field.FieldType == TemplatedFieldType.FILTER_DATE_BETWEEN_TO)
            is { } dateFieldTo)
            switch (dateFieldTo.Placement)
            {
                case TemplatedFieldPlacement.URL:
                    resultUri = resultUri.ResolveField(dateFieldTo.FieldName,
                        queryRange.End.ToString(dateFieldTo.FormatString));
                    break;
                case TemplatedFieldPlacement.BODY:
                    resultBody = resultBody.ResolveField(dateFieldTo.FieldName,
                        queryRange.End.ToString(dateFieldTo.FormatString));
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

        var pageField =
            templatedFields.FirstOrDefault(field => field.FieldType == TemplatedFieldType.RESPONSE_PAGE);

        if (pageField == null)
            throw new ApplicationException(
                "No field in either body or request url has a `page` parameter, though API is initialized as paged");

        // reset query range when pagination has been finished
        if (!pageResolver.Next(paginatedResponse))
        {
            queryRange = null;
            return (Option<Uri>.None, requestMethod, Option<string>.None);
        }

        switch (pageField.Placement)
        {
            case TemplatedFieldPlacement.URL:
                resultUri = pageResolver.ResolvePage(resultUri, pageField);
                break;
            case TemplatedFieldPlacement.BODY:
                resultBody = pageResolver.ResolvePage(resultBody, pageField);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        return (new Uri(resultUri.GetResolvedRequestElement()), requestMethod,
            resultBody.GetResolvedRequestElement());
    }

    /// <inheritdoc cref="IPaginatedApiUriProvider.HasReadAllPages"/>
    public bool HasReadAllPages()
    {
        return queryRange == null;
    }

    /// <inheritdoc cref="IRestApiUriProvider.BaseUri"/>
    public Uri BaseUri { get; }

    /// <summary>
    /// Adds page resolver to the URI provider.
    /// </summary>
    /// <param name="resolverConfiguration">Page resolver configuration</param>
    public PagedUriProvider WithPageResolver(PageResolverConfiguration resolverConfiguration)
    {
        switch (resolverConfiguration.ResolverType)
        {
            case PageResolverType.COUNTER:
                pageResolver = new PageCountingResolver(resolverConfiguration.ResolverPropertyKeyChain);
                break;
            case PageResolverType.OFFSET:
                if (!resolverConfiguration.ResponseSize.HasValue)
                    throw new ApplicationException($"Response size is required when using {PageResolverType.OFFSET}");
                pageResolver = new PageOffsetResolver(resolverConfiguration.ResponseSize.Value,
                    resolverConfiguration.ResolverPropertyKeyChain);
                break;
            case PageResolverType.TOKEN:
                pageResolver = new PageNextTokenResolver(resolverConfiguration.ResolverPropertyKeyChain);
                break;
        }

        return this;
    }

    private record TimestampRange(DateTimeOffset Start, DateTimeOffset End);
}
