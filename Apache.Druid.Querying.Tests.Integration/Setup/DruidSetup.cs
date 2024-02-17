
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Model.Compose;
using Ductus.FluentDocker.Services;
using System.Net;

[SetUpFixture]
internal class DruidSetup
{
    public static readonly Uri RouterUri = new("http://localhost:8888/");

    private ICompositeService? service = null;

    [OneTimeSetUp]
    public void SetUp()
    {
        service = new Builder()
            .UseContainer()
            .UseCompose()
            .FromFile(Path.Combine(Directory.GetCurrentDirectory(), "Setup", "druid-docker-compose.yml"))
            .AssumeComposeVersion(ComposeVersion.V2)
            .WaitForHttp(
                "router", 
                RouterUri.AbsoluteUri + "status", 
                continuation: (response, _) => response.Code < HttpStatusCode.InternalServerError ? 0 : (long)TimeSpan.FromSeconds(0.5).TotalMilliseconds)
            .Build();
        service.Start();
    }

    [OneTimeTearDown]
    public void TearDown()
        => service?.Dispose();
}
