using System.Diagnostics.CodeAnalysis;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Buffers;
using System.Buffers.Text;
using Apache.Druid.Querying.Internal.Json;
using System.Globalization;
using System.Text;

namespace Apache.Druid.Querying.Json
{
    public static class DefaultSerializerOptions
    {
        public static JsonSerializerOptions Create() => new(JsonSerializerDefaults.Web)
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            Converters =
            {
                new JsonStringEnumConverter(),
                new PolymorphicSerializer<IFilter>(),
                new PolymorphicSerializer<IMetric>(),
                new PolymorphicSerializer<IHaving>(),
                new PolymorphicSerializer<ILimitSpec>(),
                new PolymorphicSerializer<ILimitSpec.OrderBy>(),
                UnixMilisecondsConverter.WithDateTimeOffset,
                UnixMilisecondsConverter.WithDateTime,
                BoolStringNumberConverter.Singleton,
                IntervalConverter.Singleton
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

        private static class UnixMilisecondsConverter
        {
            public static readonly UnixMilisecondsConverter<DateTimeOffset> WithDateTimeOffset = new(
                DateTimeOffset.FromUnixTimeMilliseconds,
                Utf8Formatter.TryFormat);
            public static readonly UnixMilisecondsConverter<DateTime> WithDateTime = new(
                static miliseconds => DateTimeOffset.FromUnixTimeMilliseconds(miliseconds).UtcDateTime,
                Utf8Formatter.TryFormat);
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
                => reader.TokenType switch
                {
                    JsonTokenType.String => reader.GetValue<T>(),
                    JsonTokenType.Number => convert(reader.GetInt64()),
                    _ => throw new NotSupportedException()
                };

            public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
            {
                Span<byte> utf8Date = stackalloc byte[29];
                tryFormat(value, utf8Date, out _, new StandardFormat('R'));
                writer.WriteStringValue(utf8Date);
            }
        }

        public sealed class BoolStringNumberConverter : JsonConverter<bool>
        {
            public static readonly BoolStringNumberConverter Singleton = new();

            private BoolStringNumberConverter()
            {
            }

            public override bool Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
                => reader.TokenType switch
                {
                    JsonTokenType.True => true,
                    JsonTokenType.False => false,
                    JsonTokenType.String when reader.ValueTextEquals("true") || reader.ValueTextEquals("True") => true,
                    JsonTokenType.String when reader.ValueTextEquals("false") || reader.ValueTextEquals("False") => false,
                    JsonTokenType.Number => reader.GetInt32() is 1,
                    _ => throw new JsonException($"Could not deserialize {reader.GetString()} to {typeof(bool)}.")
                };

            public override void Write(Utf8JsonWriter writer, bool value, JsonSerializerOptions options)
                => writer.WriteBooleanValue(value);
        }

        public sealed class IntervalConverter : JsonConverter<Interval>
        {
            public static readonly IntervalConverter Singleton = new();
            private static readonly string separator = "/";
            private static readonly byte[] separatorBytes = Encoding.UTF8.GetBytes(separator);

            private IntervalConverter()
            {
            }

            public override Interval? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                static InvalidOperationException Invalid(ReadOnlySpan<byte> value, Exception? inner = null)
                    => new($"Invalid interval {Encoding.UTF8.GetString(value)}.", inner);
                ReadOnlySpan<byte> value = reader.HasValueSequence ?
                     reader.ValueSequence.ToArray() :
                     reader.ValueSpan;
                var separatorPosition = value.IndexOf(separatorBytes);
                if (separatorPosition is -1)
                    throw Invalid(value);

                try
                {
                    var bLeft = value[..separatorPosition];
                    var bRight = value[(separatorPosition + separatorBytes.Length)..];
                    var sLeft = Encoding.UTF8.GetString(bLeft);
                    var sRight = Encoding.UTF8.GetString(bRight);
                    var tLeft = DateTimeOffset.Parse(sLeft, CultureInfo.InvariantCulture);
                    var tRight = DateTimeOffset.Parse(sRight, CultureInfo.InvariantCulture);
                    return new(tLeft, tRight);
                }
                catch (Exception exception)
                {
                    throw Invalid(value, exception);
                }
            }

            public override void Write(Utf8JsonWriter writer, Interval value, JsonSerializerOptions options)
            {
                static string ToIsoString(DateTimeOffset t) => t.ToString("o", CultureInfo.InvariantCulture);
                var @string = $"{ToIsoString(value.From)}/{ToIsoString(value.To)}";
                writer.WriteStringValue(@string);
            }
        }
    }
}
