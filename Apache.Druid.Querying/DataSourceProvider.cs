using Apache.Druid.Querying.DependencyInjection;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using MappingBuilders = Apache.Druid.Querying.Internal.PropertyColumnNameMapping.ImmutableBuilder;

namespace Apache.Druid.Querying
{
    public sealed record Lookup<TKey, TValue>(
        [property: DataSourceColumn("k")] TKey Key, [property: DataSourceColumn("v")] TValue Value);

    public abstract class DataSourceProvider : IDataSourceInitializer
    {
        DataSourceOptions? IDataSourceInitializer.options { get; set; }
        private DataSourceOptions Options => (this as IDataSourceInitializer).Options;

        public virtual DataSource<TSource> Inline<TSource>(IEnumerable<TSource> rows, OnMapQueryToJson? onMap = null)
        {
            var allMappings = MappingBuilders.Create<TSource>();
            var mappings = allMappings.Get<TSource>();
            var properties = typeof(TSource).GetProperties();
            var columnNames = properties
                .Select(property => mappings
                    .FirstOrDefault(mapping => mapping.Property == property.Name)?.ColumnName ?? property.Name);
            JsonArray Map(TSource element)
            {
                var result = new JsonArray();
                foreach (var property in properties)
                {
                    var value = property.GetValue(element);
                    result.Add(JsonSerializer.SerializeToNode(value, property.PropertyType, Options.Serializer));
                }
                return result;
            }
            JsonArray MapAll()
            {
                var result = new JsonArray();
                foreach (var row in rows)
                {
                    result.Add(Map(row));
                }
                return result;
            }

            return New<TSource>(
                onMap,
                allMappings,
                () => new JsonObject
                {
                    ["type"] = "inline",
                    ["rows"] = MapAll(),
                    ["columnNames"] = JsonSerializer.SerializeToNode(columnNames, Options.Serializer)
                });
        }

        protected virtual DataSource<TSource> Table<TSource>(string id, OnMapQueryToJson? onMap = null)
            => New<TSource>(onMap, MappingBuilders.Create<TSource>(), () => id);

        protected virtual DataSource<Lookup<TKey, TValue>> Lookup<TKey, TValue>(string id, OnMapQueryToJson? onMap = null)
            => New<Lookup<TKey, TValue>>(
                onMap,
                MappingBuilders.Create<Lookup<TKey, TValue>>(),
                () => new JsonObject
                {
                    ["type"] = "lookup",
                    [nameof(id)] = id
                });

        private DataSource<TSource> New<TSource>(
            OnMapQueryToJson? onMap, MappingBuilders mappings, DataSourceJsonProvider createJson)
        {
            var @new = new DataSource<TSource>();
            @new.Initialize(new(
                new(() => Options),
                onMap ??= static (_, _) => { },
                createJson,
                mappings,
                SectionAtomicity: null));
            return @new;
        }
    }
}
