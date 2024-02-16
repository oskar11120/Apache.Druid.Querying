using Apache.Druid.Querying.Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace Apache.Druid.Querying.Tests.Integration
{
    internal static class ServiceProvider
    {
        public static IServiceProvider Services { get; } = Create();

        private static IServiceProvider Create() => new ServiceCollection()
            .AddDataSourceProvider<EcDruid>(new("https://druid.emesh03.internal.digitalenterpriseconnect.com/"))
            .Services
            .BuildServiceProvider();
    }
}
