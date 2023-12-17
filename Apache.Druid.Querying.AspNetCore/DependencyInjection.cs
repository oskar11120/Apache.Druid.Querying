using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Druid.Querying.AspNetCore
{
    public interface IDataSourceFactory<TService>
    {
        TService Create(string id, IQueryExecutor queryExecutor);
    }

    public class DruidQueryingBuilder
    {
        private readonly Func<IServiceProvider, JsonSerializerOptions, IQueryExecutor> queryExecutorFactory;

        public DruidQueryingBuilder(
            IHttpClientBuilder clientBuilder,
            JsonSerializerOptions serializerOptions,
            Func<IServiceProvider, JsonSerializerOptions, IQueryExecutor> queryExecutorFactory)
        {
            ClientBuilder = clientBuilder;
            SerializerOptions = serializerOptions;
            Services = clientBuilder.Services;
            this.queryExecutorFactory = queryExecutorFactory;
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

        public DruidQueryingBuilder AddDataSource<TService>(
            string id, 
            Func<IServiceProvider, IDataSourceFactory<TService>> getFactory)
            where TService : class
        {
            Services.AddSingleton(provider => getFactory(provider).Create(
                id,
                queryExecutorFactory(provider, SerializerOptions)));
            return this;
        }

        public DruidQueryingBuilder AddDataSource<TService, TSource, TFactory>(string id)
            where TService : DataSource<TSource>
            where TFactory : class, IDataSourceFactory<TService>
        {
            Services.AddSingleton<TFactory>();
            return AddDataSource(id, provider => provider.GetRequiredService<TFactory>());
        }

        public DruidQueryingBuilder AddDataSource<TSource>(string id)
            => AddDataSource(id, provider => DefaultDataSourceFactory<TSource>.Singleton);

        private sealed class DefaultDataSourceFactory<TSource> : IDataSourceFactory<DataSource<TSource>>
        {
            public static readonly DefaultDataSourceFactory<TSource> Singleton = new();

            private DefaultDataSourceFactory()
            {                   
            }

            public DataSource<TSource> Create(string id, IQueryExecutor queryExecutor) => new(id, queryExecutor);
        }
    }

    public static class ServiceCollectionExtensions
    {
        public static DruidQueryingBuilder AddDruidQuerying(this IServiceCollection services)
        {
            var clientId = Guid.NewGuid().ToString();
            var clientBuilder = services.AddHttpClient(clientId);
            var serlializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                Converters =
                {
                    new JsonStringEnumConverter(),
                    new PolymorphicSerializer<Filter>(),
                    new PolymorphicSerializer<Aggregator>(),
                    new PolymorphicSerializer<PostAggregator>(),
                    new PolymorphicSerializer<VirtualColumn>()
                }
            };
            return new(
                clientBuilder,
                serlializerOptions,
                (provider, serializerOptions) => new QueryExecutor(
                    provider.GetRequiredService<IHttpClientFactory>(),
                    serializerOptions,
                    clientId));
        }

        private sealed class PolymorphicSerializer<T> : JsonConverter<T> where T : class
        {
            public override T Read(
                ref Utf8JsonReader reader,
                Type typeToConvert,
                JsonSerializerOptions options)
                => throw new NotSupportedException();

            public override void Write(
                Utf8JsonWriter writer,
                [DisallowNull] T value,
                JsonSerializerOptions options)
                => JsonSerializer.Serialize(writer, value, value.GetType(), options);
        }
    }
}
