using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace Arcane.Framework.Sources.CdmChangeFeedSource.Extensions;

/// <summary>
/// Contains operations for parsing CSV files.
/// </summary>
internal static class CsvOperations
{
    /// <summary>
    /// Splits a CSV line into a string array, using provided header count and a delimiter.
    /// </summary>
    /// <param name="line">Line to parse.</param>
    /// <param name="headerCount">Number of expected headers.</param>
    /// <param name="delimiter">Delimiter used in the line.</param>
    /// <returns>A string array of individual values.</returns>
    public static string[] ParseCsvLine(string line, int headerCount, char delimiter = ',')
    {
        var result = new string[headerCount];
        var fieldCounter = 0;
        var prevCharIndex = 0;
        var quoteSum = 0;

        for (var ix_ch = 0; ix_ch < line.Length; ++ix_ch)
        {
            // detect quoted fields
            if (line[ix_ch] == '"' && ix_ch < line.Length - 1 && quoteSum == 0)
            {
                quoteSum += 1;
            }
            else if (line[ix_ch] == '"')
            {
                quoteSum -= 1;
            }

            // hit a delimiter - decide what to do
            if ((line[ix_ch] == delimiter || ix_ch == line.Length - 1) && quoteSum == 0)
            {
                if (line[prevCharIndex] == '"')
                {
                    prevCharIndex += 1;
                }

                if (ix_ch == prevCharIndex)
                {
                    result[fieldCounter] = line[prevCharIndex] == delimiter ? null : line[ix_ch].ToString();
                }
                else
                {
                    if (line[ix_ch] == delimiter && ix_ch == line.Length - 1)
                    {
                        var endIndex = line[ix_ch - 1] == '"' ? ix_ch - 1 : ix_ch;
                        result[fieldCounter] = line[prevCharIndex..endIndex];
                    }
                    else
                    {
                        var endIndex = ix_ch == line.Length - 1 && line[ix_ch] != '"' ? ix_ch + 1 :
                            line[ix_ch - 1] == '"' ? ix_ch - 1 : ix_ch;
                        result[fieldCounter] = line[prevCharIndex..endIndex];
                    }
                }

                fieldCounter += 1;
                prevCharIndex = ix_ch + 1;
            }
        }

        if (quoteSum != 0)
        {
            throw new InvalidOperationException(
                $"CSV line {line} with delimiter {delimiter} has mismatching field quotes");
        }

        return result;
    }

    /// <summary>
    /// Checks if a CSV line has correct number of closing/opening quotes.
    /// </summary>
    /// <param name="csvLine"></param>
    /// <returns></returns>
    public static bool IsComplete(string csvLine)
    {
        return csvLine.Count(ch => ch == '"') % 2 == 0;
    }

    /// <summary>
    /// Replaces newline characters found between quotes with empty strings.
    /// </summary>
    /// <param name="csvLine">A CSV line to purge newlines from.</param>
    /// <returns>A CSV line without newline characters inside column values.</returns>
    public static string ReplaceQuotedNewlines(string csvLine)
    {
        return Regex.Replace(csvLine, "\"[^\"]*(?:\"\"[^\"]*)*\"", m => m.Value.Replace("\n", "")).Replace("\r", "");
    }
}
