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
                    .WaitAndRetryAsync(60, _ => TimeSpan.FromSeconds(1))))
            .Services
            .AddHttpClient()
            .BuildServiceProvider();

        [OneTimeSetUp]
        protected static async Task SetUp()
        {
            // Not disposed so to keep it running in between tests.
            _ = DruidSetup.StartContainers();
            await DruidSetup.IngestWikipediaEdits(Services.GetRequiredService<IHttpClientFactory>(), Wikipedia);
        }
    }
}
