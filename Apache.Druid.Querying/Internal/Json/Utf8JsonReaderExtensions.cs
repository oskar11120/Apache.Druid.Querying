using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Apache.Druid.Querying.Internal.Json
{
    internal static class Utf8JsonReaderExtensions
    {
        public static bool ReadToToken(this ref Utf8JsonReader reader, JsonTokenType ofType)
        {
            while (reader.Read())
                if (reader.TokenType == ofType)
                    return true;
            return false;
        }

        public static bool ReadToProperty(this ref Utf8JsonReader reader, ReadOnlySpan<byte> name)
        {
            while (reader.ReadToToken(JsonTokenType.PropertyName))
                if (reader.ValueTextEquals(name))
                    return true;
            return false;
        }

        public static bool ReadThroughAllOfGreaterThanCurrentDepth(this ref Utf8JsonReader reader)
        {
            if (reader.TokenType is JsonTokenType.False or JsonTokenType.True or JsonTokenType.String or JsonTokenType.Number or JsonTokenType.Null)
                return true;

            if (!reader.Read())
                return false;

            var depth = reader.CurrentDepth;
            while (reader.Read())
                if (reader.CurrentDepth == depth)
                    return true;

            return false;
        }

        public static bool ReadToPropertyValue<T>(this ref Utf8JsonReader reader, ReadOnlySpan<byte> name, [NotNullWhen(true)] out T value)
        {
            if ((reader.TokenType is not JsonTokenType.PropertyName && !reader.ReadToProperty(name)) || !reader.Read())
            {
                value = default!;
                return false;
            }

            var type = typeof(T);
            if (type == typeof(bool))
            {
                var result = reader.GetBoolean();
                value = Unsafe.As<bool, T>(ref result);
            }
            else if (type == typeof(string))
            {
                var result = reader.GetString() ?? throw new ArgumentException("BAD JSON");
                value = Unsafe.As<string, T>(ref result);
            }
            else if (type == typeof(DateTimeOffset))
            {
                var result = reader.GetDateTimeOffset();
                value = Unsafe.As<DateTimeOffset, T>(ref result);
            }
            else if(type == typeof(double))
            {
                var result = reader.GetDouble();
                value = Unsafe.As<double, T>(ref result);
            }
            else if (type == typeof(int))
            {
                var result = reader.GetInt32();
                value = Unsafe.As<int, T>(ref result);
            }
            else if (type == typeof(long))
            {
                var result = reader.GetInt64();
                value = Unsafe.As<long, T>(ref result);
            }
            else if (type == typeof(DateTime))
            {
                var result = reader.GetDateTime();
                value = Unsafe.As<DateTime, T>(ref result);
            }
            else if (type == typeof(decimal))
            {
                var result = reader.GetDecimal();
                value = Unsafe.As<decimal, T>(ref result);
            }
            else if (type == typeof(short))
            {
                var result = reader.GetInt16();
                value = Unsafe.As<short, T>(ref result);
            }
            else if (type == typeof(Guid))
            {
                var result = reader.GetGuid();
                value = Unsafe.As<Guid, T>(ref result);
            }
            else if (type == typeof(float))
            {
                var result = reader.GetSingle();
                value = Unsafe.As<float, T>(ref result);
            }
            else
            {
                throw new NotSupportedException("Unsupported type");
            }

#pragma warning disable CS8762 // Parameter must have a non-null value when exiting in some condition.
            return true;
#pragma warning restore CS8762 // Parameter must have a non-null value when exiting in some condition.
        }
    }
}
