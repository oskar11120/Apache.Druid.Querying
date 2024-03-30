using Apache.Druid.Querying.Internal;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Json
{
    public static class QueryExtensions
    {
        private static readonly JsonSerializerOptions defaultSerializerOptions = DefaultSerializerOptions.Create();
        private static readonly ConcurrentDictionary<Type, MethodInfo[]> cache = new();

        internal static JsonObject MapToJson(
            this IQueryWith.State query,
            JsonSerializerOptions serializerOptions,
            IColumnNameMappingProvider columNames)
        {
            if (!cache.TryGetValue(query.GetType(), out var methods))
                methods = query
                   .GetStateInterfaces()
                   .Select(@interface => @interface.GetMethod(nameof(IQueryWithInternal.State<None>.AddToJson), BindingFlags.NonPublic | BindingFlags.Instance))
                   .ToArray()!;
            var result = new JsonObject();
            var parameters = new object[] { result, serializerOptions, columNames };
            foreach (var method in methods)
                method.Invoke(query, parameters);
            return result;
        }

        public static JsonObject MapToJson<TSource>(
            this IQueryWithSource<TSource> query,
            JsonSerializerOptions? serializerOptions = null)
            => MapToJson(
                query,
                serializerOptions ?? defaultSerializerOptions,
                IColumnNameMappingProvider.ImmutableBuilder.Create<TSource>());
    }
}
