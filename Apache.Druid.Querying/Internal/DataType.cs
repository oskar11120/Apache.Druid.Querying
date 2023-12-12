using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Druid.Querying.Internal
{
    internal static class DataType
    {
        private const string String = "String";
        private const char
            L = '<',
            R = '>';
        private static readonly Dictionary<Type, string> simpleMap = new[]
        {
            (new[] { typeof(string), typeof(Guid), typeof(char), typeof(Uri), typeof(Enum) }, String),
            (new[] { typeof(double) }, "Double"),
            (new[] { typeof(float) }, "Float"),
            (new[] { typeof(short), typeof(int), typeof(long), typeof(DateTime), typeof(DateTimeOffset) }, "Long")
        }
        .SelectMany(pair => pair.Item1.Select(type => KeyValuePair.Create(type, pair.Item2)))
        .ToDictionary(pair => pair.Key, pair => pair.Value);

        public static string Get<TValue>()
        {
            var result = new StringBuilder();
            Set(typeof(TValue), result);
            return result.ToString();
        }

        private static void Set(Type type, StringBuilder result)
        {
            if (TrySetSimple(type, result))
                return;
            else if (TrySetNullable(type, result))
                return;
            else if (TrySetArray(type, result))
                return;
            else if (TrySetComplex(type, result))
                return;
            throw new NotSupportedException($"No matching {nameof(DataType)} found for {nameof(type)}.");
        }

        private static bool TrySetSimple(Type type, StringBuilder result)
        {
            simpleMap.TryGetValue(type, out var simple);
            simple ??= type is { IsPrimitive: true, IsEnum: true } ? String : null;
            if (simple is not null)
                result.Append(simple);
            return simple is not null;
        }

        private static bool TrySetNullable(Type type, StringBuilder result)
        {
            var isNullable = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
            if (!isNullable)
                return false;

            var argumentType = type.GetGenericArguments().Single();
            Set(argumentType, result);
            return true;
        }

        private static bool TrySetArray(Type type, StringBuilder result)
        {
            if (!type.IsGenericType || type.GetInterface(nameof(IEnumerable)) is null)
                return false;

            var argumentType = type.GetGenericArguments().Single();
            result.Append("Array").Append(L);
            Set(argumentType, result);
            result.Append(R);
            return true;
        }

        private static bool TrySetComplex(Type type, StringBuilder result)
        {
            var properties = type.GetProperties();
            if (properties.Length == 0)
                return false;

            result.Append("Complex").Append(L).Append("json").Append(R);
            return true;
        }
    }
}
