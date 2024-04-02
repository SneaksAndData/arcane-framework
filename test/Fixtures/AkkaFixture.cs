using Akka.Actor;
using Akka.Streams;

namespace Arcane.Framework.Tests.Fixtures;

public class AkkaFixture
{
    public AkkaFixture()
    {
        ActorSystem = ActorSystem.Create(nameof(AkkaFixture));
        Materializer = ActorSystem.Materializer();
    }

    public ActorSystem ActorSystem { get; }
    public IMaterializer Materializer { get; }
}
