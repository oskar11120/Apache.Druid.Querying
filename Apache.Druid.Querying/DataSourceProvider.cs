using Apache.Druid.Querying.DependencyInjection;
using System;
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

        protected virtual DataSource<TSource> Inline<TSource, TDataSource>(
            IEnumerable<TSource> rows, Func<TDataSource> factory, OnMapQueryToJson? onMap = null)
            where TDataSource : DataSource<TSource>
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
                    result.Add(JsonSerializer.SerializeToNode(value, property.PropertyType, Options.DataSerializer));
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

            return New<TSource, TDataSource>(
                onMap,
                allMappings,
                () => new JsonObject
                {
                    ["type"] = "inline",
                    ["rows"] = MapAll(),
                    ["columnNames"] = JsonSerializer.SerializeToNode(columnNames, Options.QuerySerializer)
                },
                factory);
        }

        public virtual DataSource<TSource> Inline<TSource>(IEnumerable<TSource> rows, OnMapQueryToJson? onMap = null)
            => Inline<TSource, DataSource<TSource>>(rows, static () => new(), onMap);

        protected virtual TDataSource Table<TSource, TDataSource>(string id, Func<TDataSource> factory, OnMapQueryToJson? onMap = null)
            where TDataSource : DataSource<TSource>
            => New<TSource, TDataSource>(onMap, MappingBuilders.Create<TSource>(), () => id, factory);

        protected virtual DataSource<TSource> Table<TSource>(string id, OnMapQueryToJson? onMap = null)
            => Table<TSource, DataSource<TSource>>(id, static () => new(), onMap);

        protected virtual TDataSource Lookup<TKey, TValue, TDataSource>(
            string id, Func<TDataSource> factory, OnMapQueryToJson? onMap = null)
            where TDataSource : DataSource<Lookup<TKey, TValue>>
            => New<Lookup<TKey, TValue>, TDataSource>(
                onMap,
                MappingBuilders.Create<Lookup<TKey, TValue>>(),
                () => new JsonObject
                {
                    ["type"] = "lookup",
                    [nameof(id)] = id
                },
                factory);

        protected virtual DataSource<Lookup<TKey, TValue>> Lookup<TKey, TValue>(string id, OnMapQueryToJson? onMap = null)
            => Lookup<TKey, TValue, DataSource<Lookup<TKey, TValue>>>(id, static () => new(), onMap);

        private TDataSource New<TSource, TDataSource>(
            OnMapQueryToJson? onMap,
            MappingBuilders mappings,
            DataSourceJsonProvider createJson,
            Func<TDataSource> factory)
            where TDataSource : DataSource<TSource>
        {
            var @new = factory();
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
