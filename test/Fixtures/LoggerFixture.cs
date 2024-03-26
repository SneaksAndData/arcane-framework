using Microsoft.Extensions.Logging;

namespace Arcane.Framework.Tests.Fixtures
{
    public class LoggerFixture
    {
        public ILoggerFactory Factory { get; }
        public LoggerFixture()
        {
            this.Factory = LoggerFactory.Create(conf => conf.AddConsole());
        }
    }
}
