using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Model.Compose;
using System.Net;

namespace Apache.Druid.Querying.Tests.Integration;

internal static class TestEnvironment
{
    public static readonly Uri DruidRouterUri = new("http://localhost:8888");
    public static readonly Uri ToxiproxyUri = new("http://localhost:8474");
    public static readonly Uri DruidRouterUri_UnderToxiproxy = new("http://localhost:50000");

    public static IDisposable CreateAndStart()
    {
        var containers = new Builder()
             .UseContainer()
             .UseCompose()
             .FromFile(Path.Combine(Directory.GetCurrentDirectory(), "TestEnvironment", "druid-docker-compose.yml"))
             .AssumeComposeVersion(ComposeVersion.V2)
             .WaitForHttp(
                 "router",
                 DruidRouterUri.AbsoluteUri + "status/health",
                 continuation: (response, _) => response.Code < HttpStatusCode.InternalServerError ? 0 : (long)TimeSpan.FromSeconds(0.5).TotalMilliseconds)
             .Build()
             .Start();
        return containers;
    }

    private sealed class CompositeDiposable : IDisposable
    {
        private readonly IDisposable one;
        private readonly IDisposable other;

        public CompositeDiposable(IDisposable one, IDisposable other)
        {
            this.one = one;
            this.other = other;
        }

        public void Dispose()
        {
            one.Dispose();
            other.Dispose();
        }
    }
}
