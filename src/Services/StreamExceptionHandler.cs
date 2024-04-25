using System;
using System.Threading.Tasks;
using Akka.Util;
using Akka.Util.Extensions;
using Arcane.Framework.Contracts;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Sources.Exceptions;
using Microsoft.Extensions.Logging;
using Snd.Sdk.Tasks;

namespace Arcane.Framework.Services;

internal class StreamExceptionHandler: IArcaneExceptionHandler
{
    private readonly ILogger<StreamExceptionHandler> logger;
    private readonly IStreamStatusService streamStatusService;
    private readonly IStreamContext streamContext;

    public StreamExceptionHandler(ILogger<StreamExceptionHandler> logger,
        IStreamStatusService streamStatusService, IStreamContext streamContext)
    {
        this.logger = logger;
        this.streamStatusService = streamStatusService;
        this.streamContext = streamContext;
    }

    private Task<Option<int>> HandleSchemaMismatch()
    {
        this.logger.LogInformation("Schema mismatch detected. Reporting schema mismatch and exiting");
        return this.streamStatusService.ReportSchemaMismatch(this.streamContext.StreamId).Map(_ => ExitCodes.SUCCESS.AsOption());
    }

    private Task<Option<int>> HandleSchemaInconsistency()
    {
        this.logger.LogInformation("Schema mismatch detected. Reporting schema mismatch and exiting");
        return Task.FromResult(ExitCodes.RESTART.AsOption());
    }

    public Task<Option<int>> HandleException(Exception exception)
    {
        return exception switch
        {
            SchemaMismatchException => this.HandleSchemaMismatch(),
            SchemaInconsistentException => this.HandleSchemaInconsistency(),
            _ => Task.FromResult(Option<int>.None)
        };
    }
}
