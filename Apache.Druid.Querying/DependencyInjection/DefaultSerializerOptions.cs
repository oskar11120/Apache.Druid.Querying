using System.Diagnostics.CodeAnalysis;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Buffers;
using Apache.Druid.Querying.Elements;

namespace Apache.Druid.Querying.DependencyInjection
{
    public static class DefaultSerializerOptions
    {
        public static JsonSerializerOptions Create() => new(JsonSerializerDefaults.Web)
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
