using Apache.Druid.Querying.Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Polly;

namespace Apache.Druid.Querying.Tests.Integration
{
    [SetUpFixture]
    internal class ServiceProvider
    {
        public static WikipediaDataSourceProvider Wikipedia => Services.GetRequiredService<WikipediaDataSourceProvider>();
        public static IServiceProvider Services { get; } = Create();

        private static IServiceProvider Create() => new ServiceCollection()
            .AddDataSourceProvider<WikipediaDataSourceProvider>(DruidSetup.RouterUri)
            .ConfigureClient(clientBuilder => clientBuilder
                .AddTransientHttpErrorPolicy(policy => policy
                    .WaitAndRetryAsync(10, _ => TimeSpan.FromSeconds(1))))
            .Services
            .AddHttpClient()
            .BuildServiceProvider();

        private static IDisposable? containers;

        [OneTimeSetUp]
        protected static async Task SetUp()
        {
            containers = DruidSetup.CreateAndStartContainers();
            await DruidSetup.IngestWikipediaEdits(Services.GetRequiredService<IHttpClientFactory>(), Wikipedia);
        }

        [OneTimeTearDown]
        protected static void TearDown()
        {
            // Not disposed so to keep them running in between test runs.
            containers = null;

            containers?.Dispose();
        }
    }
}
