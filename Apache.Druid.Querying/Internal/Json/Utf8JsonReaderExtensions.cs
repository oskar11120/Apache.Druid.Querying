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

        public static bool ReadToToken(this ref Utf8JsonReader reader, JsonTokenType ofType, int atDepth)
        {
            while (reader.ReadToToken(ofType))
                if (reader.CurrentDepth == atDepth)
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

        public static TValue GetValue<TValue>(this ref Utf8JsonReader reader)
        {
            var type = typeof(TValue);
            if (type == typeof(bool))
            {
                var result = reader.GetBoolean();
                return Unsafe.As<bool, TValue>(ref result);
            }
            else if (type == typeof(string))
            {
                var result = reader.GetString();
                return Unsafe.As<string, TValue>(ref result!);
            }
            else if (type == typeof(DateTimeOffset))
            {
                var result = reader.GetDateTimeOffset();
                return Unsafe.As<DateTimeOffset, TValue>(ref result);
            }
            else if (type == typeof(double))
            {
                var result = reader.GetDouble();
                return Unsafe.As<double, TValue>(ref result);
            }
            else if (type == typeof(int))
            {
                var result = reader.GetInt32();
                return Unsafe.As<int, TValue>(ref result);
            }
            else if (type == typeof(long))
            {
                var result = reader.GetInt64();
                return Unsafe.As<long, TValue>(ref result);
            }
            else if (type == typeof(DateTime))
            {
                var result = reader.GetDateTime();
                return Unsafe.As<DateTime, TValue>(ref result);
            }
            else if (type == typeof(decimal))
            {
                var result = reader.GetDecimal();
                return Unsafe.As<decimal, TValue>(ref result);
            }
            else if (type == typeof(short))
            {
                var result = reader.GetInt16();
                return Unsafe.As<short, TValue>(ref result);
            }
            else if (type == typeof(Guid))
            {
                var result = reader.GetGuid();
                return Unsafe.As<Guid, TValue>(ref result);
            }
            else if (type == typeof(float))
            {
                var result = reader.GetSingle();
                return Unsafe.As<float, TValue>(ref result);
            }
            else
            {
                throw new NotSupportedException("Unsupported type");
            }
        }

        public static bool ReadToPropertyValue<TValue>(this ref Utf8JsonReader reader, ReadOnlySpan<byte> name, [NotNullWhen(true)] out TValue value)
        {
            if (!reader.ReadToProperty(name) || !reader.Read())
            {
                value = default!;
                return false;
            }

            value = reader.GetValue<TValue>()!;
            return true;
        }
    }
}
