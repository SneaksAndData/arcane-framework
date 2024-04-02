using System;
using System.Collections.Generic;
using System.Linq;

namespace Arcane.Framework.Sources.RestApi.Services;

/// <summary>
/// REST API template resolver for templated fields.
/// </summary>
public sealed class RestApiTemplate
{
    private readonly string baseTemplate;
    private readonly List<string> templatedFieldNames;
    private List<string> remainingFieldNames;
    private string resolvedTemplate;

    /// <summary>
    /// Constructor for the REST API template resolver.
    /// </summary>
    /// <param name="baseTemplate">The template string</param>
    /// <param name="templatedFieldNames">Templated fields names</param>
    public RestApiTemplate(string baseTemplate, List<string> templatedFieldNames)
    {
        this.baseTemplate = baseTemplate;
        this.templatedFieldNames = templatedFieldNames;
    }

    private bool IsEmpty => string.IsNullOrEmpty(this.baseTemplate);

    /// <summary>
    /// Create a resolver for the template.
    /// </summary>
    public RestApiTemplate CreateResolver()
    {
        if (this.IsEmpty)
        {
            return this;
        }

        this.resolvedTemplate = this.baseTemplate;
        this.remainingFieldNames = this.templatedFieldNames.Select(v => v).ToList();

        return this;
    }

    /// <summary>
    /// Create an empty REST API template.
    /// </summary>
    public static RestApiTemplate Empty()
    {
        return new RestApiTemplate(null, new List<string>());
    }

    /// <summary>
    /// Courtesy of https://stackoverflow.com/questions/36759694/is-there-a-string-format-that-can-accept-named-input-parameters-instead-of-ind
    /// </summary>
    /// <param name="fieldName"></param>
    /// <param name="fieldValue"></param>
    /// <returns></returns>
    public RestApiTemplate ResolveField(string fieldName, string fieldValue)
    {
        if (this.IsEmpty)
        {
            return this;
        }

        if (this.remainingFieldNames.Contains(fieldName))
        {
            var parameters = new Dictionary<string, object> { { $"@{fieldName}", fieldValue } };
            this.resolvedTemplate = parameters.Aggregate(this.resolvedTemplate,
                (current, parameter) => current.Replace(parameter.Key, parameter.Value.ToString()));
            this.remainingFieldNames.Remove(fieldName);
        }

        return this;
    }

    /// <summary>
    /// Return the resolved request element.
    /// </summary>
    /// <returns>The resolved template as string</returns>
    /// <exception cref="ApplicationException">Thrown when there are unresolved fields</exception>
    public string GetResolvedRequestElement()
    {
        if (this.IsEmpty)
        {
            return string.Empty;
        }

        if (this.remainingFieldNames.Count > 0)
        {
            throw new ApplicationException(
                $"Cannot return the resolved template as there are unresolved fields: {string.Join(",", this.remainingFieldNames)}");
        }

        return this.resolvedTemplate;
    }
}
