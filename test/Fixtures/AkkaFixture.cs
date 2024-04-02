using Akka.Actor;
using Akka.Streams;

namespace Arcane.Framework.Tests.Fixtures;

public class AkkaFixture
{
    public AkkaFixture()
    {
        this.ActorSystem = ActorSystem.Create(nameof(AkkaFixture));
        this.Materializer = this.ActorSystem.Materializer();
    }

    public ActorSystem ActorSystem { get; }
    public IMaterializer Materializer { get; }
}
