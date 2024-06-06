using System;
using System.IO;

namespace Arcane.Framework.Tests.Extensions;

public static class StringExtensions
{
    public static string ToSampleCdmChangeFeedSchemaPath(this string entityName)
    {
        return Path.Join(
            AppDomain.CurrentDomain.BaseDirectory,
            "Sources",
            "SampleData",
            "CdmChangeFeed",
            entityName,
            $"{entityName}.cdm.json"
        );
    }

    public static string ToSampleCdmEntitySchemaPath(this string entityName)
    {
        return Path.Join(
            AppDomain.CurrentDomain.BaseDirectory,
            "Sources",
            "SampleData",
            "BaseEntity",
            entityName,
            $"{entityName}.cdm.json"
        );
    }
}
