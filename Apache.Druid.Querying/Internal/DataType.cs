using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;

namespace Apache.Druid.Querying.Internal
{
    internal static class DataType
    {
        private const char
            L = '<',
            R = '>';
        private static readonly Dictionary<Type, SimpleDataType> simpleMap = new[]
        {
            (new[] { typeof(string), typeof(Guid), typeof(char), typeof(Uri), typeof(Enum), typeof(bool) }, SimpleDataType.String),
            (new[] { typeof(double) }, SimpleDataType.Double),
            (new[] { typeof(float) }, SimpleDataType.Float),
            (new[] { typeof(short), typeof(int), typeof(long), typeof(DateTime), typeof(DateTimeOffset) }, SimpleDataType.Long)
        }
        .SelectMany(pair => pair.Item1.Select(type => KeyValuePair.Create(type, pair.Item2)))
        .ToDictionary(pair => pair.Key, pair => pair.Value);

        public static string Get<TValue>()
        {
            var result = new StringBuilder();
            Set(typeof(TValue), result);
            return result.ToString();
        }

        public static SimpleDataType GetSimple(Type type)
            => IsNullable(type, out var argumentType) ?
            GetSimple(argumentType) :
            TryGetSimple(type, out var simple) ?
                simple :
                throw new InvalidOperationException($"No matching {nameof(SimpleDataType)} {nameof(DataType)} exists for {nameof(type)} {type}.");

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

        private static bool TryGetSimple(Type type, out SimpleDataType result)
        {
            if (simpleMap.TryGetValue(type, out SimpleDataType simple))
            {
                result = simple;
                return true;
            }

            if (type is { IsPrimitive: true, IsEnum: true })
            {
                result = SimpleDataType.String;
                return true;
            }

            result = default;
            return false;
        }

        private static bool TrySetNullable(Type type, StringBuilder result)
        {
            if (IsNullable(type, out var argumentType))
            {
                Set(argumentType, result);
                return true;
            }

            return false;
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

        private static bool IsNullable(Type type, [MaybeNullWhen(false)] out Type argumentType)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                argumentType = type.GetGenericArguments()[0];
                return true;
            }

            argumentType = null;
            return false;
        }
    }
}
