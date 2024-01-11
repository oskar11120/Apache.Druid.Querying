using Apache.Druid.Querying.DependencyInjection;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using MappingBuilders = Apache.Druid.Querying.IColumnNameMappingProvider.ImmutableBuilder;

namespace Apache.Druid.Querying
{
    public readonly record struct Lookup<TKey, TValue>(
        [property: DataSourceColumn("k")] TKey Key, [property: DataSourceColumn("v")] TValue Value);

    [SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "For consistency public api.")]
    public abstract class DataSourceProvider : IDataSourceInitializer
    {
        DataSourceOptions? IDataSourceInitializer.options { get; set; }
        private DataSourceOptions Options => (this as IDataSourceInitializer).Options;

        public DataSource<TSource> Inline<TSource>(IEnumerable<TSource> rows)
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

            return Create<TSource>(
                allMappings,
                () => new JsonObject
                {
                    ["type"] = "inline",
                    ["rows"] = MapAll(),
                    ["columnNames"] = JsonSerializer.SerializeToNode(columnNames, Options.Serializer)
                });
        }

        protected DataSource<TSource> Table<TSource>(string id)
            => Create<TSource>(MappingBuilders.Create<TSource>(), () => id);

        protected DataSource<Lookup<TKey, TValue>> Lookup<TKey, TValue>(string id)
            => Create<Lookup<TKey, TValue>>(
                MappingBuilders.Create<Lookup<TKey, TValue>>(),
                () => new JsonObject
                {
                    ["type"] = "lookup",
                    [nameof(id)] = id
                });

        public DataSource<Union<TFirst, TSecond>> Union<TFirst, TSecond>(DataSource<TFirst> first, DataSource<TSecond> second)
            => first.Union(second);

        public DataSource<Union<TFirst, TSecond, TThird>> Union<TFirst, TSecond, TThird>(
            DataSource<TFirst> first, DataSource<TSecond> second, DataSource<TThird> third)
            => first.Union(second, third);

        public DataSource<TResult> Query<TSource, TResult>(DataSource<TSource> dataSource, IQueryWithSource<TSource>.AndResult<TResult> query)
            => dataSource.WrapQuery(query);

        public DataSource<TResult> Query<TSource, TResult, TMapper>(
            DataSource<TSource> dataSource, IQueryWithSource<TSource>.AndMappedResult<TResult, TMapper> query)
            where TMapper : IQueryResultMapper<TResult>, new()
            => dataSource.WrapQuery(query);

        public DataSource<InnerJoinResult<TLeft, TRight>> InnerJoin<TLeft, TRight>(DataSource<TLeft> left, DataSource<TRight> right, string rightPrefix, string condition)
            => left.InnerJoin(right, rightPrefix, condition);

        public DataSource<LeftJoinResult<TLeft, TRight>> LeftJoin<TLeft, TRight>(DataSource<TLeft> left, DataSource<TRight> right, string rightPrefix, string condition)
            => left.LeftJoin(right, rightPrefix, condition);

        private DataSource<TSource> Create<TSource>(MappingBuilders mappings, DataSourceJsonProvider createJson)
            => new(() => Options, createJson, mappings);
    }
}
