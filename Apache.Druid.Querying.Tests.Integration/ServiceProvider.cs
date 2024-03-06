using Apache.Druid.Querying.Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Polly;

namespace Apache.Druid.Querying.Tests.Integration
{
    internal static class ServiceProvider
    {
        public static IServiceProvider Services { get; } = Create();

        private static IServiceProvider Create() => new ServiceCollection()
            .AddDataSourceProvider<WikipediaDataSourceProvider>(DruidSetup.RouterUri)
            .ConfigureClient(clientBuilder => clientBuilder
                .AddTransientHttpErrorPolicy(policy => policy
                    .WaitAndRetryAsync(60, _ => TimeSpan.FromSeconds(1))))
            .Services
            .BuildServiceProvider();
    }
}
