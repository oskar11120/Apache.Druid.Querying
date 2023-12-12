using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Druid.Querying.Internal
{
    internal static class DataType
    {
        public enum Simple
        {
            String,
            Double,
            Float,
            Long
        }

        private const char
            L = '<',
            R = '>';
        private static readonly Dictionary<Type, Simple> simpleMap = new[]
        {
            (new[] { typeof(string), typeof(Guid), typeof(char), typeof(Uri), typeof(Enum) }, Simple.String),
            (new[] { typeof(double) }, Simple.Double),
            (new[] { typeof(float) }, Simple.Float),
            (new[] { typeof(short), typeof(int), typeof(long), typeof(DateTime), typeof(DateTimeOffset) }, Simple.Long)
        }
        .SelectMany(pair => pair.Item1.Select(type => KeyValuePair.Create(type, pair.Item2)))
        .ToDictionary(pair => pair.Key, pair => pair.Value);

        public static string Get<TValue>()
        {
            var result = new StringBuilder();
            Set(typeof(TValue), result);
            return result.ToString();
        }

        public static Simple GetSimple<TValue>()
        {
            var type = typeof(TValue);
            return TryGetSimple(type, out var simple) ?
                simple :
                throw new InvalidOperationException($"No matching {nameof(Simple)} {nameof(DataType)} exists for {nameof(type)}.");
        }

        private static void Set(Type type, StringBuilder result)
        {
            if (TryGetSimple(type, out var simple))
            {
                result.Append(simple.ToString());
                return;
            }
            else if (TrySetNullable(type, result))
                return;
            else if (TrySetArray(type, result))
                return;
            else if (TrySetComplex(type, result))
                return;
            throw new NotSupportedException($"No matching {nameof(DataType)} exists for {nameof(type)}.");
        }

        private static bool TryGetSimple(Type type, out Simple result)
        {
            if (simpleMap.TryGetValue(type, out Simple simple))
            {
                result = simple;
                return true;
            }

            if (type is { IsPrimitive: true, IsEnum: true })
            {
                result = Simple.String;
                return true;
            }

            result = default;
            return false;
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
