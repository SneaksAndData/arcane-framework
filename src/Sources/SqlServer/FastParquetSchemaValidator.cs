using System.Collections.Generic;
using System.Linq;
using Akka;
using Akka.Streams.Dsl;
using Arcane.Framework.Sinks.Parquet.Exceptions;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.Base;
using Parquet.Data;

namespace Arcane.Framework.Sources.SqlServer;

public class FastParquetSchemaValidator : ISchemaValidator<List<DataCell>>
{
    private readonly Schema schema;

    public FastParquetSchemaValidator(Schema schema)
    {
        this.schema = schema;
    }

    public Flow<List<DataCell>, List<DataCell>, TMat> Validate<TMat>()
    {
        return Flow.Create<List<DataCell>, TMat>().Select(this.ValidateCellGroup);
    }

    private List<DataCell> ValidateCellGroup(List<DataCell> cellGroups)
    {
        if (cellGroups.Count > 0)
        {
            var (srcFields, sinkFields) = (cellGroups.Count, this.schema.GetDataFields().Length);
            if (srcFields != sinkFields)
            {
                throw new SchemaInconsistentException(srcFields, sinkFields);
            }
        }

        return cellGroups;
    }
}
