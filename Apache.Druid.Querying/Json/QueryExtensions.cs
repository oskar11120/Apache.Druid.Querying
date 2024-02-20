using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Json
{
    public static class QueryExtensions
    {
        private static readonly JsonSerializerOptions @default = DefaultSerializerOptions.Create();

        internal static JsonObject MapToJson<TSource>(
            this IQueryWithSource<TSource> query,
            JsonSerializerOptions? serializerOptions,
            IColumnNameMappingProvider.ImmutableBuilder? columNameMappings)
        {
            serializerOptions ??= @default;
            columNameMappings ??= IColumnNameMappingProvider.ImmutableBuilder.Create<TSource>();
            columNameMappings = query.GetColumnNameMappings() is IColumnNameMappingProvider.ImmutableBuilder existing ?
                columNameMappings.Combine(existing) : columNameMappings;
            var result = new JsonObject();
            var state = query.GetState();
            foreach (var (key, factory) in state)
                result.Add(key, factory(serializerOptions, columNameMappings));
            return result;
        }

        public static JsonObject MapToJson<TSource>(
            this IQueryWithSource<TSource> query,
            JsonSerializerOptions? serializerOptions = null)
            => MapToJson(query, serializerOptions, null);
    }
}
