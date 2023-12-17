using Microsoft.Extensions.DependencyInjection;

namespace Apache.Druid.Querying.AspNetCore.Tests.Integration
{
    internal static class ServiceProvider
    {
        public static IServiceProvider Services { get; } = Create();

        private static IServiceProvider Create() => new ServiceCollection()
            .AddDruidQuerying(new("https://druid.emesh03.internal.digitalenterpriseconnect.com/"))
            .AddDataSource<Message>("data-variables")
            .Services
            .BuildServiceProvider();
    }
}
