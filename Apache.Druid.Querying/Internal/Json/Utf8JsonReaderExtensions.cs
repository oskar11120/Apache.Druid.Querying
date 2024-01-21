using System;
using System.Text.Json;

namespace Apache.Druid.Querying.Internal.Json
{
    internal static class Utf8JsonReaderExtensions
    {
        public static bool ReadToToken(this ref Utf8JsonReader reader, JsonTokenType ofType)
        {
            var found = false;
            while (reader.Read())
                if (reader.TokenType == ofType)
                {
                    found = true;
                    break;
                }

            return found;
        }

        public static bool ReadToProperty(this ref Utf8JsonReader reader, ReadOnlySpan<byte> name)
        {
            while (reader.ReadToToken(JsonTokenType.PropertyName))
                if (reader.ValueTextEquals(name))
                    return true;

            return false;
        }
    }
}
