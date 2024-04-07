using System.Diagnostics.CodeAnalysis;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Buffers;
using System.Buffers.Text;
using Apache.Druid.Querying.Internal.Json;
using System.Globalization;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

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
                IntervalConverter.Singleton,
                GranularityConverter.Singleton
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
                    _ => throw new JsonException($"Could not convert {reader.GetString()} to {typeof(T)}.")
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
        private static string ToSnake(this string @string)
            => Regex.Replace(Regex.Replace(@string, "(.)([A-Z][a-z]+)", "$1_$2"), "([a-z0-9])([A-Z])", "$1_$2").ToLower();
        public sealed class GranularityConverter : JsonConverter<Granularity>
        {
            private enum Property
            {
                TimeZone,
                Origin,
                Period,
                Duration
            }

            private static class Properties
            {
                public static readonly byte[] Type = Encoding.UTF8.GetBytes("type");
                public static readonly byte[] TimeZone = Encoding.UTF8.GetBytes("timeZone");
                public static readonly byte[] Origin = Encoding.UTF8.GetBytes("origin");
                public static readonly byte[] Period = Encoding.UTF8.GetBytes("period");
                public static readonly byte[] Duration = Encoding.UTF8.GetBytes("duration");

                public static readonly (Property Property, byte[] Utf8Bytes)[] AllExceptForType = new[]
                {
                    (Property.TimeZone, TimeZone),
                    (Property.Origin, Origin),
                    (Property.Period, Period),
                    (Property.Duration, Duration)
                };
            }

            public static readonly GranularityConverter Singleton = new();

            private GranularityConverter()
            {
            }

            private static readonly Dictionary<SimpleGranularity, string> simpleGranularityStringMap = Enum
                .GetValues<SimpleGranularity>()
                .ToDictionary(granularity => granularity, granularity => granularity.ToString().TrimEnd('s').ToSnake());

            private static readonly Dictionary<string, SimpleGranularity> stringSimpleGranularityMap =
                simpleGranularityStringMap.ToDictionary(pair => pair.Value, pair => pair.Key);

            private static readonly Dictionary<SimpleGranularity, string> simpleGranularityPeriodMap = new()
            {
                [SimpleGranularity.Second] = "PT1S",
                [SimpleGranularity.Minute] = "PT1M",
                [SimpleGranularity.FiveMinutes] = "PT5M",
                [SimpleGranularity.TenMinutes] = "PT10M",
                [SimpleGranularity.FifteenMinutes] = "PT15M",
                [SimpleGranularity.ThirtyMinutes] = "PT30M",
                [SimpleGranularity.Hour] = "PT1H",
                [SimpleGranularity.SixHours] = "PT6H",
                [SimpleGranularity.EightHours] = "PT8H",
                [SimpleGranularity.Day] = "P1D",
                [SimpleGranularity.Week] = "P1W",
                [SimpleGranularity.Month] = "P1M",
                [SimpleGranularity.Quarter] = "P3M",
                [SimpleGranularity.Year] = "P1Y"
            };

            public override Granularity? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                static InvalidOperationException Invalid(string? reason = null) => new($"Json convtained invalid {nameof(Granularity)}. {reason}");
                if (reader.TokenType is JsonTokenType.Null)
                    return null;
                if (reader.TokenType is not JsonTokenType.StartObject)
                    throw Invalid();

                static bool ReadToPropertyValue(ref Utf8JsonReader reader, out Property result)
                {
                    reader.Read();
                    if (reader.TokenType is JsonTokenType.EndObject) 
                    {
                        result = default;
                        return false;
                    }

                    if (reader.TokenType is not JsonTokenType.PropertyName)
                        throw Invalid();

                    foreach (var (property, bytes) in Properties.AllExceptForType)
                        if (reader.ValueTextEquals(bytes))
                        {
                            reader.Read();
                            result = property;
                            return true;
                        }

                    result = default;
                    return false;
                }

                if (!reader.ReadToPropertyValue<string>(Properties.Type, out var type))
                    return null;
                var isDuration = type.Equals(nameof(Granularity.Duration), StringComparison.OrdinalIgnoreCase);
                var isPeriod = !isDuration && type.Equals(nameof(Granularity.Period), StringComparison.OrdinalIgnoreCase);
                SimpleGranularity? simple = isDuration || isPeriod ?
                    null :
                    stringSimpleGranularityMap.TryGetValue(type, out var simple_) ?
                        simple_ :
                        throw Invalid($"Unexpected {nameof(type)}: {type}.");

                TimeSpan? duration = null;
                string? period = null;
                string? timeZone = null;
                DateTimeOffset? origin = null;
                while (ReadToPropertyValue(ref reader, out var property))
                {
                    switch (property)
                    {
                        case Property.TimeZone:
                            timeZone = reader.GetString();
                            break;
                        case Property.Origin:
                            origin = JsonSerializer.Deserialize<DateTimeOffset>(ref reader, options);
                            break;
                        case Property.Period:
                            period = reader.GetString();
                            break;
                        case Property.Duration:
                            duration = TimeSpan.FromMilliseconds(reader.GetInt32());
                            break;
                        default:
                            throw new NotSupportedException();
                    }
                }

                if (reader.TokenType is not JsonTokenType.EndObject)
                    throw Invalid();
                return new(simple, duration, period, timeZone, origin);
            }

            public override void Write(Utf8JsonWriter writer, Granularity value, JsonSerializerOptions options)
            {
                var hasTimeZoneOrOrigin = value.TimeZone is not null || value.Origin is not null;
                if ((!hasTimeZoneOrOrigin && value.Simple is not null) || value.Simple is SimpleGranularity.All or SimpleGranularity.None)
                {
                    writer.WriteStringValue(simpleGranularityStringMap[value.Simple.Value]);
                    return;
                }

                void WritePeriod(string period)
                {
                    writer.WriteString(Properties.Type, Properties.Period);
                    writer.WriteString(Properties.Period, period);
                }

                writer.WriteStartObject();

                if (value.Simple is SimpleGranularity simple)
                    WritePeriod(simpleGranularityPeriodMap[simple]);
                else if (value.Period is string period)
                    WritePeriod(period);
                else if (value.Duration is TimeSpan duration)
                {
                    writer.WriteString(Properties.Type, Properties.Duration);
                    writer.WriteNumber(Properties.Duration, (int)duration.TotalMilliseconds);
                }

                if (value.TimeZone is string timeZone)
                    writer.WriteString(Properties.TimeZone, timeZone);
                if (value.Origin is DateTimeOffset origin)
                    writer.WriteString(Properties.Origin, origin);

                writer.WriteEndObject();
            }
        }
    }
}
