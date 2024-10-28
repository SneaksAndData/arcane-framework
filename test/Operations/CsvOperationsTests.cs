using System;
using Arcane.Framework.Sources.Extensions;
using Xunit;

namespace Arcane.Framework.Tests.Operations;

public class CsvOperationsTests
{
    [Theory]
    [InlineData(true, "\"qv1\",\"qv2\",\"qv3\",,\"qv4\",\"qv5\",\"qv6\",123,,0.12345", "qv1", "qv2", "qv3", null, "qv4",
        "qv5", "qv6", "123", null, "0.12345")]
    [InlineData(true, "123,,\"qv1\",,,,", "123", null, "qv1", null, null, null)]
    [InlineData(true, ",,123,341,5", null, null, "123", "341", "5")]
    [InlineData(true, "\"q\",,\"1321\"", "q", null, "1321")]
    [InlineData(false, "\"q\",\",\"1321\"", "q", null, "1321")]
    [InlineData(true, "\"q\",,\"13,21\"", "q", null, "13,21")]
    [InlineData(true, "123,,\", abc def\"", "123", null, ", abc def")]
    [InlineData(true, "5637144576,\"NFO\",,0,", "5637144576", "NFO", null, "0", null)]
    public void ParseCsvLine(bool isValid, string line, params string[] expectedArray)
    {
        if (isValid)
        {
            var parsed = CsvOperations.ParseCsvLine(line, expectedArray.Length);

            Assert.Equal(expectedArray, parsed);
        }
        else
        {
            Assert.Throws<InvalidOperationException>(() => CsvOperations.ParseCsvLine(line, expectedArray.Length));
        }
    }
}
