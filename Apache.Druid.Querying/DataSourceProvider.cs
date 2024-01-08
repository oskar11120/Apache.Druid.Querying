using Apache.Druid.Querying.DependencyInjection;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying
{
    public readonly record struct Lookup<TKey, TValue>(TKey K, TValue V);
    public readonly record struct Union<TFirst, TSecond>(TFirst First, TSecond Second);
    public readonly record struct Union<TFirst, TSecond, TThird>(TFirst First, TSecond Second, TThird Third);

    public abstract class DataSourceProvider : IDataSourceInitializer
    {
        DataSourceOptions? IDataSourceInitializer.options { get; set; }
        private DataSourceOptions Options => (this as IDataSourceInitializer).Options;

        public DataSource<Union<TFirst, TSecond>> Union<TFirst, TSecond>(DataSource<TFirst> first, DataSource<TSecond> second)
            => Create<Union<TFirst, TSecond>>(() => new JsonObject
            {
                [Constants.type] = Constants.union,
                [Constants.dataSources] = new JsonArray
                {
                    first.GetJsonRepresentation(),
                    second.GetJsonRepresentation()
                }
            });

        public DataSource<Union<TFirst, TSecond, TThird>> Union<TFirst, TSecond, TThird>(
            DataSource<TFirst> first, DataSource<TSecond> second, DataSource<TThird> third)
            => Create<Union<TFirst, TSecond, TThird>>(() => new JsonObject
            {
                [Constants.type] = Constants.union,
                [Constants.dataSources] = new JsonArray
                {
                    first.GetJsonRepresentation(),
                    second.GetJsonRepresentation(),
                    third.GetJsonRepresentation()
                }
            });

        public DataSource<TSource> Inline<TSource>(IEnumerable<TSource> rows)
            => Create<TSource>(() => new JsonObject
            {
                [Constants.type] = "inline",
                [Constants.dataSources] = JsonSerializer.SerializeToNode(rows, Options.Serializer) // TODO Map property names to column names?
            });

        public DataSource<TResult> Query<TSource, TResult>(DataSource<TSource> dataSource, IQueryWithSource<TSource>.AndResult<TResult> query)
            => dataSource.WrapQuery(query);

        public DataSource<TResult> Query<TSource, TResult, TMapper>(
            DataSource<TSource> dataSource, IQueryWithSource<TSource>.AndMappedResult<TResult, TMapper> query)
            where TMapper : IQueryResultMapper<TResult>, new()
            => dataSource.WrapQuery(query);

        protected DataSource<TSource> Table<TSource>(string id)
            => Create<TSource>(() => id);

        protected DataSource<Lookup<TKey, TValue>> Lookup<TKey, TValue>(string id)
            => Create<Lookup<TKey, TValue>>(() => new JsonObject
            {
                [Constants.type] = "lookup",
                [nameof(id)] = id
            });

        private DataSource<TSource> Create<TSource>(DataSourceJsonProvider createJson)
            => new(() => Options, createJson);

        private static class Constants
        {
            public static readonly string type = nameof(type);
            public static readonly string union = nameof(union);
            public static readonly string dataSources = nameof(dataSources);
        }
    }
}
