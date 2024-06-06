using System.Collections.Generic;
using Akka.Streams.Dsl;
using Arcane.Framework.Sources.Base;
using Arcane.Framework.Sources.Exceptions;
using Parquet.Data;
using ParquetColumn = Parquet.Data.DataColumn;

namespace Arcane.Framework.Sources.SqlServer;

/// <summary>
/// Validates the schema of a Parquet data source
/// </summary>
public class FastParquetSchemaValidator : ISchemaValidator<List<ParquetColumn>>
{
    private readonly Schema schema;

    /// <summary>
    /// Creates a new Parquet schema validator
    /// </summary>
    /// <param name="schema">Parquet schema</param>
    public FastParquetSchemaValidator(Schema schema)
    {
        this.schema = schema;
    }

    /// <inheritdoc />
    public Flow<List<ParquetColumn>, List<ParquetColumn>, TMat> Validate<TMat>() =>
        Flow.Create<List<ParquetColumn>, TMat>().Select(this.ValidateCellGroup);

    private List<ParquetColumn> ValidateCellGroup(List<ParquetColumn> cellGroups)
    {
        if (cellGroups.Count <= 0)
        {
            return cellGroups;
        }

        var (srcFields, sinkFields) = (cellGroups.Count, this.schema.GetDataFields().Length);
        if (srcFields != sinkFields)
        {
            throw new SchemaInconsistentException(srcFields, sinkFields);
        }

        return cellGroups;
    }
}
