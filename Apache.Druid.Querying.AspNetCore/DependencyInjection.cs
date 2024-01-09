using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;
using Apache.Druid.Querying.Json;
using Apache.Druid.Querying.DependencyInjection;

namespace Apache.Druid.Querying.AspNetCore
{
    public class DataSourceProviderBuilder
    {
        public DataSourceProviderBuilder(IHttpClientBuilder clientBuilder, JsonSerializerOptions serializerOptions)
        {
            ClientBuilder = clientBuilder;
            SerializerOptions = serializerOptions;
            Services = clientBuilder.Services;
        }

        public IServiceCollection Services { get; }
        public IHttpClientBuilder ClientBuilder { get; }
        public JsonSerializerOptions SerializerOptions { get; }

        public DataSourceProviderBuilder ConfigureClient(Action<IHttpClientBuilder> configure)
        {
            configure(ClientBuilder);
            return this;
        }

        public DataSourceProviderBuilder ConfigureSerializer(Action<JsonSerializerOptions> configure)
        {
            configure(SerializerOptions);
            return this;
        }
    }

    public static class ServiceCollectionExtensions
    {
        public static DataSourceProviderBuilder AddDataSourceProvider<TProvider>(
            this IServiceCollection services, Uri druidApiUri)
            where TProvider : DataSourceProvider
        {
            var clientId = Guid.NewGuid().ToString();
            var clientBuilder = services.AddHttpClient(clientId);
            clientBuilder.ConfigureHttpClient(client => client.BaseAddress = druidApiUri);
            var serlializerOptions = DefaultSerializerOptions.Create();
            services
                .AddSingleton<IDataSourceInitializer, TProvider>()
                .AddSingleton(services =>
                {
                    var all = services.GetServices<IDataSourceInitializer>();
                    var match = all
                        .Where(initializer => initializer.GetType() == typeof(TProvider))
                        .ToArray();
                    var matchSingle = match.Length == 1 ?
                        match[0] :
                        throw new InvalidOperationException($"{typeof(TProvider)} has been registered multiple times");
                    var factory = services.GetRequiredService<IHttpClientFactory>();
                    matchSingle.Initialize(new(serlializerOptions, () => factory.CreateClient(clientId)));
                    return (TProvider)matchSingle;
                });
            return new(clientBuilder, serlializerOptions);
        }
    }
}
