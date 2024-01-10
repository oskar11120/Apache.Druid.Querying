using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Internal;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using MappingBuilders = Apache.Druid.Querying.IColumnNameMappingProvider.ImmutableBuilder;

namespace Apache.Druid.Querying
{
    public readonly record struct Lookup<TKey, TValue>(
        [property: DataSourceColumn("k")] TKey Key, [property: DataSourceColumn("v")] TValue Value);

    public readonly record struct Union<TFirst, TSecond>(TFirst? First, TSecond? Second) 
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<Union<TFirst, TSecond>>
        {
            public Union<TFirst, TSecond> Deserialize(ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TFirst>(),
                    context.Deserialize<TSecond>());
        }
    }

    public readonly record struct Union<TFirst, TSecond, TThird>(TFirst? First, TSecond? Second, TThird? Third) 
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<Union<TFirst, TSecond, TThird>>
        {
            public Union<TFirst, TSecond, TThird> Deserialize(ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TFirst>(),
                    context.Deserialize<TSecond>(),
                    context.Deserialize<TThird>());
        }
    }

    public abstract class DataSourceProvider : IDataSourceInitializer
    {
        DataSourceOptions? IDataSourceInitializer.options { get; set; }
        private DataSourceOptions Options => (this as IDataSourceInitializer).Options;

        public DataSource<Union<TFirst, TSecond>> Union<TFirst, TSecond>(DataSource<TFirst> first, DataSource<TSecond> second)
            => Create<Union<TFirst, TSecond>>(
                first.ColumnNameMappings.Combine(second.ColumnNameMappings),
                () => new JsonObject
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
            => Create<Union<TFirst, TSecond, TThird>>(
                first.ColumnNameMappings.Combine(second.ColumnNameMappings).Combine(third.ColumnNameMappings),
                () => new JsonObject
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
            => Create<TSource>(
                MappingBuilders.Create<TSource>(),
                () => new JsonObject
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

        public DataSource<InnerJoinResult<TLeft, TRight>> InnerJoin<TLeft, TRight>(DataSource<TLeft> left, DataSource<TRight> right, string rightPrefix, string condition)
            => left.InnerJoin(right, rightPrefix, condition);

        public DataSource<LeftJoinResult<TLeft, TRight>> LeftJoin<TLeft, TRight>(DataSource<TLeft> left, DataSource<TRight> right, string rightPrefix, string condition)
            => left.LeftJoin(right, rightPrefix, condition);

        protected DataSource<TSource> Table<TSource>(string id)
            => Create<TSource>(MappingBuilders.Create<TSource>(), () => id);

        protected DataSource<Lookup<TKey, TValue>> Lookup<TKey, TValue>(string id)
            => Create<Lookup<TKey, TValue>>(
                MappingBuilders.Create<Lookup<TKey, TValue>>(),
                () => new JsonObject
                {
                    [Constants.type] = "lookup",
                    [nameof(id)] = id
                });

        private DataSource<TSource> Create<TSource>(MappingBuilders mappings, DataSourceJsonProvider createJson)
            => new(() => Options, createJson, mappings);

        private static class Constants
        {
            public static readonly string type = nameof(type);
            public static readonly string union = nameof(union);
            public static readonly string dataSources = nameof(dataSources);
        }
    }
}
