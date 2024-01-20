﻿using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Json
{
    public static class QueryExtensions
    {
        private static readonly JsonSerializerOptions @default = DefaultSerializerOptions.Create();

        public static JsonObject MapToJson<TSource>(
            this IQueryWithSource<TSource> query,
            JsonSerializerOptions? serializerOptions = null,
            IColumnNameMappingProvider? columNames = null)
        {
            serializerOptions ??= @default;
            columNames ??= IColumnNameMappingProvider.ImmutableBuilder.Create<TSource>();
            var result = new JsonObject();
            var state = query.GetState();
            foreach (var (key, factory) in state)
                result.Add(key, factory(serializerOptions, columNames));
            return result;
        }
    }
}