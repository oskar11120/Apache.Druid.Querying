using Microsoft.Extensions.DependencyInjection;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Elements;

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
            Services
                .AddSingleton<IDataSourceInitializer<TSource>, TImplementation>()
                .AddSingleton(provider =>
                {
                    var factory = provider.GetRequiredService<IHttpClientFactory>();
                    var state = new DataSourceInitlializationState(id, SerializerOptions, () => factory.CreateClient(clientId));
                    var implementation = provider.GetRequiredService<IDataSourceInitializer<TSource>>();
                    implementation.Initialize(state);
                    return (TService)implementation;
                });
            return this;
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
            var serlializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                Converters =
                {
                    new JsonStringEnumConverter(),
                    new PolymorphicSerializer<Filter>(),
                    new PolymorphicSerializer<Aggregator>(),
                    new PolymorphicSerializer<PostAggregator>(),
                    new PolymorphicSerializer<VirtualColumn>(),
                //    new UnixMilisecondsConverter<DateTimeOffset>(
                //        DateTimeOffset.FromUnixTimeMilliseconds,
                //        Utf8Formatter.TryFormat),
                //    new UnixMilisecondsConverter<DateTime>(
                //        static miliseconds => DateTimeOffset.FromUnixTimeMilliseconds(miliseconds).UtcDateTime,
                //        Utf8Formatter.TryFormat)
                }
            };
            return new(clientBuilder, serlializerOptions, clientId);
        }

        private sealed class PolymorphicSerializer<T> : JsonConverter<T> where T : class
        {
            private JsonSerializerOptions? withoutPolymorphicSerializer;

            public override T Read(
                ref Utf8JsonReader reader,
                Type typeToConvert,
                JsonSerializerOptions options)
                => throw new NotSupportedException();

            public override void Write(
                Utf8JsonWriter writer,
                [DisallowNull] T value,
                JsonSerializerOptions options)
            {
                var type = value.GetType();
                if (type == typeof(T))
                {
                    if (withoutPolymorphicSerializer is null)
                    {

                        withoutPolymorphicSerializer = new(options);
                        withoutPolymorphicSerializer.Converters.Remove(this);
                    }

                    options = withoutPolymorphicSerializer;
                }

                JsonSerializer.Serialize(writer, value, type, options);
            }
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
