using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Arcane.Framework.Contracts;
using Arcane.Framework.Services.Base;
using Snd.Sdk.Kubernetes.Base;

namespace Arcane.Framework.Services;

[ExcludeFromCodeCoverage(Justification = "Implementation is specific to Kubernetes API, should be tested in integration tests.")]
internal class StreamStatusService: IStreamStatusService
{
    private readonly IKubeCluster kubernetesService;
    public StreamStatusService(IKubeCluster kubernetesService)
    {
        this.kubernetesService = kubernetesService;
    }

    public Task ReportSchemaMismatch(string streamId)
    {
        var nameSpace = this.kubernetesService.GetCurrentNamespace();
        return this.kubernetesService.AnnotateJob(streamId,
            nameSpace,
            Annotations.STATE_ANNOTATION_KEY,
            Annotations.SCHEMA_MISMATCH_STATE_ANNOTATION_VALUE);
    }
}
