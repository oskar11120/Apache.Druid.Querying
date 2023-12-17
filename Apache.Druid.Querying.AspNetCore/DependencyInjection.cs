using Microsoft.Extensions.DependencyInjection;
using System.Buffers.Text;
using System.Buffers;
using System.Diagnostics;
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
        public static DruidQueryingBuilder AddDruidQuerying(
            this IServiceCollection services,
            Uri druidApiUri)
        {
            var clientId = Guid.NewGuid().ToString();
            var clientBuilder = services.AddHttpClient(clientId);
            clientBuilder.ConfigureHttpClient(client => client.BaseAddress = druidApiUri);
            var serlializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                Converters =
                {
                    new JsonStringEnumConverter(),
                    new PolymorphicSerializer<Filter>(),
                    new PolymorphicSerializer<Aggregator>(),
                    new PolymorphicSerializer<PostAggregator>(),
                    new PolymorphicSerializer<VirtualColumn>(),
                    new UnixMilisecondsConverter<DateTimeOffset>(
                        DateTimeOffset.FromUnixTimeMilliseconds,
                        Utf8Formatter.TryFormat),
                    new UnixMilisecondsConverter<DateTimeOffset>(
                        static miliseconds => DateTimeOffset.FromUnixTimeMilliseconds(miliseconds).UtcDateTime,
                        Utf8Formatter.TryFormat)
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

        private sealed class UnixMilisecondsConverter<T> : JsonConverter<T>
        {
            public delegate bool TryFormatUTf8(T value, Span<byte> destination, out int bytesWritten, StandardFormat format);

            private readonly Func<long, T> convert;
            private readonly TryFormatUTf8 tryFormat;

            public UnixMilisecondsConverter(Func<long, T> convert, TryFormatUTf8 tryFormat)
            {
                this.convert = convert;
                this.tryFormat = tryFormat;
            }

            public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                var number = reader.TokenType switch
                {
                    JsonTokenType.Number => reader.GetInt64(),
                    JsonTokenType.String => long.Parse(reader.GetString()!),
                    _ => throw new NotSupportedException()
                };
                return convert(number);
            }

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                // The "R" standard format will always be 29 bytes.
                Span<byte> utf8Date = stackalloc byte[29];
                tryFormat(value, utf8Date, out _, new StandardFormat('R'));
                writer.WriteStringValue(utf8Date);
            }
        }
    }
}
