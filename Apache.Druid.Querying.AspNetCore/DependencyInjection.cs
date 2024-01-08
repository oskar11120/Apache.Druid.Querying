using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;
using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Json;

namespace Apache.Druid.Querying.AspNetCore
{
    public class DruidQueryingBuilder
    {
        private readonly string clientId;

        public DruidQueryingBuilder(
            IHttpClientBuilder clientBuilder,
            JsonSerializerOptions serializerOptions,
            string clientId)
        {
            ClientBuilder = clientBuilder;
            SerializerOptions = serializerOptions;
            Services = clientBuilder.Services;
            this.clientId = clientId;
        }

        public IServiceCollection Services { get; }
        public IHttpClientBuilder ClientBuilder { get; }
        public JsonSerializerOptions SerializerOptions { get; }

        public DruidQueryingBuilder ConfigureClient(Action<IHttpClientBuilder> configure)
        {
            configure(ClientBuilder);
            return this;
        }

        public DruidQueryingBuilder ConfigureSerializer(Action<JsonSerializerOptions> configure)
        {
            configure(SerializerOptions);
            return this;
        }

        public DruidQueryingBuilder AddDataSource<TService, TImplementation, TSource>(string id)
            where TService : DataSource<TSource>
            where TImplementation : TService
        {
            throw new NotImplementedException();
            //Services
            //    .AddSingleton<IDataSourceInitializer<TSource>, TImplementation>()
            //    .AddSingleton(provider =>
            //    {
            //        var factory = provider.GetRequiredService<IHttpClientFactory>();
            //        var state = new DataSourceInitlializationState(id, SerializerOptions, () => factory.CreateClient(clientId));
            //        var implementation = provider.GetRequiredService<IDataSourceInitializer<TSource>>();
            //        implementation.Initialize(state);
            //        return (TService)implementation;
            //    });
            //return this;
        }

        public DruidQueryingBuilder AddDataSource<TService, TSource>(string id) where TService : DataSource<TSource>
            => AddDataSource<TService, TService, TSource>(id);

        public DruidQueryingBuilder AddDataSource<TSource>(string id)
            => AddDataSource<DataSource<TSource>, TSource>(id);
    }

    public static class ServiceCollectionExtensions
    {
        public static DruidQueryingBuilder AddDruidQuerying(
            this IServiceCollection services,
            Uri druidApiUri)
        {
            var clientId = Guid.NewGuid().ToString();
            var clientBuilder = services.AddHttpClient(clientId);
            clientBuilder.ConfigureHttpClient(client => client.BaseAddress = druidApiUri);
            var serlializerOptions = DefaultSerializerOptions.Create();
            return new(clientBuilder, serlializerOptions, clientId);
        }
    }
}
