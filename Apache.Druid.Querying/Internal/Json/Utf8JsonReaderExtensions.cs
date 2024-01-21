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

        public static bool ReadToPropertyValue<T>(this ref Utf8JsonReader reader, ReadOnlySpan<byte> name, [NotNullWhen(true)] out T value)
        {
            var found = reader.ReadToProperty(name);
            if (!found || !reader.Read())
            {
                value = default!;
                return false;
            }

            var type = typeof(T);
            if (type == typeof(bool))
            {
                var @bool = reader.GetBoolean();
                value = Unsafe.As<bool, T>(ref @bool);
            }
            else if (type == typeof(string))
            {
                var @string = reader.GetString() ?? throw new ArgumentException("BAD JSON");
                value = Unsafe.As<string, T>(ref @string);
            }
            else if (type == typeof(DateTimeOffset))
            {
                var t = reader.GetDateTimeOffset();
                value = Unsafe.As<DateTimeOffset, T>(ref t);
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
