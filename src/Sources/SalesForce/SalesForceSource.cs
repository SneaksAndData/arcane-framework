using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net.Http;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Stage;
using Akka.Util;
using Arcane.Framework.Contracts;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Sinks.Parquet.Models;
using Arcane.Framework.Sources.Base;
using Arcane.Framework.Sources.Exceptions;
using Arcane.Framework.Sources.SalesForce.Models;
using Arcane.Framework.Sources.SalesForce.Services.AuthenticatedMessageProviders;
using Arcane.Framework.Sources.SalesForce.Exceptions;
using Parquet.Data;

namespace Arcane.Framework.Sources.SalesForce;

/// <summary>
/// Source for reading data from SalesForce BULK v2 API.
/// </summary>
public class SalesForceSource : GraphStage<SourceShape<List<DataCell>>>, IParquetSource, ITaggedSource
{
    private readonly string entityName;
    private readonly SalesForceJobProvider jobProvider;
    private readonly HttpClient httpClient;

    private readonly TimeSpan changeCaptureInterval;

    private SalesForceSource(
        SalesForceJobProvider jobProvider,
        string entityName,
        TimeSpan changeCaptureInterval
        )
    {
        this.jobProvider = jobProvider;
        this.httpClient = new HttpClient();
        this.entityName = entityName;
        this.changeCaptureInterval = changeCaptureInterval;

        this.Shape = new SourceShape<List<DataCell>>(this.Out);
    }


    /// <summary>
    /// Only use this constructor for unit tests to mock http calls.
    /// </summary>
    /// <param name="jobProvider">Salesforce Job Provider</param>
    /// <param name="entityName">Name of Salesforce entity</param>
    /// <param name="httpClient">Http client for making requests</param>
    /// <param name="changeCaptureInterval">How often to track changes</param>
    /// <returns></returns>
    private SalesForceSource(

        SalesForceJobProvider jobProvider,
        string entityName,
        HttpClient httpClient,
        TimeSpan changeCaptureInterval
        ) : this(jobProvider, entityName, changeCaptureInterval)
    {
        this.httpClient = httpClient;
    }


    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.InitialAttributes"/>
    protected override Attributes InitialAttributes { get; } = Attributes.CreateName(nameof(SalesForceSource));

    /// <summary>
    /// Source outlet
    /// </summary>
    public Outlet<List<DataCell>> Out { get; } = new($"{nameof(SalesForceSource)}.Out");

    /// <inheritdoc cref="GraphStageWithMaterializedValue{TShape,TMaterialized}.Shape"/>
    public override SourceShape<List<DataCell>> Shape { get; }

    /// <inheritdoc cref="IParquetSource.GetParquetSchema"/>
    public Schema GetParquetSchema()
    {
        var schema = this.jobProvider.GetSchema(httpClient, this.entityName).Result;

        return schema.Value.GetReader().ToParquetSchema();

    }

    /// <inheritdoc cref="ITaggedSource.GetDefaultTags"/>
    public SourceTags GetDefaultTags()
    {
        return new SourceTags
        {
            SourceEntity = this.entityName,
            SourceLocation = this.jobProvider.accountName

        };
    }

    /// <summary>
    /// Creates a new instance of <see cref="SalesForceSource"/>
    /// </summary>
    /// <param name="jobProvider"></param>
    /// <param name="httpClient"></param>
    /// <param name="entityName"></param>
    /// <param name="changeCaptureInterval"></param>
    public static SalesForceSource Create(
        SalesForceJobProvider jobProvider,
        HttpClient httpClient,
        string entityName,
        TimeSpan changeCaptureInterval
     )
    {
        return new SalesForceSource(jobProvider
            , entityName, httpClient, changeCaptureInterval);
    }



    /// <inheritdoc cref="GraphStage{TShape}.CreateLogic"/>
    protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
    {
        return new SourceLogic(this);
    }

    private sealed class SourceLogic : TimerGraphStageLogic
    {
        private const string TimerKey = nameof(SalesForceSource);
        private readonly LocalOnlyDecider decider;
        private readonly HttpClient httpClient;
        private readonly SalesForceSource source;
        private SalesForceEntity entitySchema;
        private Option<SalesForceJob> currentJob;



        public SourceLogic(SalesForceSource source) : base(source.Shape)
        {
            this.source = source;

            this.httpClient = this.source.httpClient ?? new HttpClient();

            this.decider = Decider.From((ex) => ex.GetType().Name switch
            {
                nameof(ArgumentException) => Directive.Stop,
                nameof(ArgumentNullException) => Directive.Stop,
                nameof(InvalidOperationException) => Directive.Stop,
                nameof(ConfigurationErrorsException) => Directive.Stop,
                nameof(ObjectDisposedException) => Directive.Stop,
                nameof(IOException) => Directive.Restart,
                nameof(TimeoutException) => Directive.Restart,
                nameof(HttpRequestException) => Directive.Restart,
                _ => Directive.Stop
            });

            this.currentJob = Option<SalesForceJob>.None;


            this.SetHandler(source.Out, this.PullChanges, this.Finish);
        }

        private void Finish(Exception cause)
        {
            if (cause is not null && cause is not SubscriptionWithCancelException.NonFailureCancellation)
            {
                this.FailStage(cause);
            }

            this.httpClient.Dispose();
        }

        public override void PreStart()
        {
            this.Log.Info("Prestart");
            this.UpdateSchema();
        }

        public void UpdateSchema()
        {
            var newSchema = this.source.jobProvider.GetSchema(httpClient, this.source.entityName).Result;

            if (newSchema.IsEmpty)
            {
                this.Log.Warning("Could not update schema");
            }
            else
            {
                var schemaUnchanged =
                    SalesForceEntity.SalesForceEntityComparer.Equals(this.entitySchema, newSchema.Value);
                this.entitySchema = (this.entitySchema == null, schemaEquals: schemaUnchanged) switch
                {
                    (true, _) or (false, true) => newSchema.Value,
                    (false, false) => throw new SchemaMismatchException()
                };
            }

        }

        private void CreateNewJob()
        {
            this.Log.Info("Creating new job");
            var job = this.source.jobProvider.CreateJob(httpClient, this.entitySchema).Result;

            if (job.IsEmpty)
            {
                this.Log.Warning("Could not create job");
            }
            else
            {
                this.currentJob = job;
            }
            this.ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
        }

        private void UpdateJobStatus()
        {
            this.Log.Info("Updating job status");
            var response = this.source.jobProvider.GetJobStatus(httpClient, this.currentJob.Value).Result;

            if (response.IsEmpty)
            {
                this.Log.Warning("Could not create job");
            }
            else
            {
                this.currentJob = response;
            }
            this.ScheduleOnce(TimerKey, TimeSpan.FromSeconds(1));
        }

        private void ProcessResult()
        {

            var rows = this.source.jobProvider.GetJobResult(httpClient, this.currentJob.Value, this.entitySchema).Result;
            this.EmitMultiple(this.source.Out, rows);

            if (!rows.Any())
            {
                this.currentJob = Option<SalesForceJob>.None;
                this.ScheduleOnce(TimerKey, this.source.changeCaptureInterval);
            }

        }

        private void PullChanges()
        {
            switch (this.currentJob.Select(job => job.Status).GetOrElse(SalesforceJobStatus.None))
            {
                case SalesforceJobStatus.UploadComplete:; this.UpdateJobStatus(); break;
                case SalesforceJobStatus.InProgress: this.UpdateJobStatus(); break;
                case SalesforceJobStatus.Aborted: this.FailStage(new SalesForceJobAbortedException($"job : {this.currentJob.Value.Id} was aborted by source")); break;
                case SalesforceJobStatus.Failed: this.FailStage(new SalesForceJobFailedException($"job : {this.currentJob.Value.Id} returned with failure")); break;
                case SalesforceJobStatus.JobComplete: this.ProcessResult(); break;
                case SalesforceJobStatus.None: this.CreateNewJob(); break;
            };
        }

        protected override void OnTimer(object timerKey)
        {
            this.PullChanges();
        }
    }
}
