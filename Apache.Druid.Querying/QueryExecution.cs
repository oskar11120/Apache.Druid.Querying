using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Json;
using Apache.Druid.Querying.Internal.Sections;
using Apache.Druid.Querying.Json;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq.Expressions;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;

namespace Apache.Druid.Querying
{
    /// <summary>
    /// Not safe to reuse. Each IQueryResultDeserializer<TResult>.Deserialize call requires a new instance.
    /// </summary>
    internal sealed record QueryResultDeserializationContext(
        JsonStreamReader Json,
        JsonSerializerOptions Options,
        SectionAtomicity.IProvider Atomicity,
        PropertyColumnNameMapping.IProvider ColumnNameMappings)
    {
        private readonly Dictionary<object, object> State = new();

        public T GetOrAddToState<T>(object key, Func<QueryResultDeserializationContext, T> factory)
            where T : notnull
        {
            if (State.TryGetValue(key, out var existing))
                return (T)existing;
            var @new = factory(this);
            State.TryAdd(key, @new);
            return @new;
        }
    }

    internal sealed class Mutable<TValue>
    {
        public TValue? Value;
    }

    internal sealed class TruncatedQueryResultHandlingContext
    {
        public object? State;
    }

    public static partial class IQueryWith
    {
        public interface Result<TResult>
        {
            internal IAsyncEnumerable<TResult> Deserialize(QueryResultDeserializationContext context, CancellationToken token);
        }

        public interface Source<TSource> : IQueryWithInternal.SectionAtomicity, IQueryWithInternal.PropertyColumnNameMappingChanges
        {
            public interface AndResult<TResult> : Source<TSource>, Result<TResult>
            {
                internal IAsyncEnumerable<TResult> OnTruncatedResultsSetQueryForRemaining(
                    IAsyncEnumerable<TResult> results, TruncatedQueryResultHandlingContext context, Mutable<Source<TSource>> setter, CancellationToken token);
            }
        }
    }

    public delegate JsonNode? DataSourceJsonProvider();

    public delegate void OnExecuteQuery(IQueryWith.State Query);

    public partial record QueryDataKind
    {
        public sealed record Source : QueryDataKind;

        public sealed record UnionFirst : QueryDataKind;
        public sealed record UnionSecond : QueryDataKind;
        public sealed record UnionThird : QueryDataKind;

        public sealed record JoinLeft : QueryDataKind;
        public sealed record JoinRight : QueryDataKind;
    }

    public interface IOptionalQueryData<out TValue, TKind> where TKind : QueryDataKind
    {
        TValue? Value { get; }
    }

    public interface IQueryData<out TValue, TKind> : IOptionalQueryData<TValue, TKind> where TKind : QueryDataKind
    {
        new TValue Value { get; }
        TValue IOptionalQueryData<TValue, TKind>.Value => Value;
    }

    public interface IDataSourceData<out TValue> : IQueryData<TValue, QueryDataKind.Source>
    {
    }

    public readonly record struct Union<TFirst, TSecond>(TFirst? First, TSecond? Second) :
        IOptionalQueryData<TFirst, QueryDataKind.UnionFirst>,
        IOptionalQueryData<TSecond, QueryDataKind.UnionSecond>
    {
        internal static readonly QueryResultElement.Deserializer<Union<TFirst, TSecond>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TFirst>(),
                    context.Deserialize<TSecond>());
        TFirst? IOptionalQueryData<TFirst, QueryDataKind.UnionFirst>.Value => First;
        TSecond? IOptionalQueryData<TSecond, QueryDataKind.UnionSecond>.Value => Second;
    }

    public readonly record struct Union<TFirst, TSecond, TThird>(TFirst? First, TSecond? Second, TThird? Third) :
        IOptionalQueryData<TFirst, QueryDataKind.UnionFirst>,
        IOptionalQueryData<TSecond, QueryDataKind.UnionSecond>,
        IOptionalQueryData<TThird, QueryDataKind.UnionThird>
    {
        internal static readonly QueryResultElement.Deserializer<Union<TFirst, TSecond, TThird>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TFirst>(),
                    context.Deserialize<TSecond>(),
                    context.Deserialize<TThird>());
        TFirst? IOptionalQueryData<TFirst, QueryDataKind.UnionFirst>.Value => First;
        TSecond? IOptionalQueryData<TSecond, QueryDataKind.UnionSecond>.Value => Second;
        TThird? IOptionalQueryData<TThird, QueryDataKind.UnionThird>.Value => Third;
    }

    public readonly record struct InnerJoinData<TLeft, TRight>(TLeft Left, TRight Right) :
        IQueryData<TLeft, QueryDataKind.JoinLeft>,
        IQueryData<TRight, QueryDataKind.JoinRight>
    {
        internal static readonly QueryResultElement.Deserializer<InnerJoinData<TLeft, TRight>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TLeft>(),
                    context.Deserialize<TRight>());
        TLeft IQueryData<TLeft, QueryDataKind.JoinLeft>.Value => Left;
        TRight IQueryData<TRight, QueryDataKind.JoinRight>.Value => Right;
    }

    public readonly record struct LeftJoinData<TLeft, TRight>(TLeft Left, TRight Right) :
        IQueryData<TLeft, QueryDataKind.JoinLeft>,
        IQueryData<TRight, QueryDataKind.JoinRight>
    {
        TLeft IQueryData<TLeft, QueryDataKind.JoinLeft>.Value => Left;
        TRight IQueryData<TRight, QueryDataKind.JoinRight>.Value => Right;
    }

    public readonly record struct LeftJoinResult<TLeft, TRight>(TLeft Left, TRight? Right) :
        IQueryData<TLeft, QueryDataKind.JoinLeft>,
        IOptionalQueryData<TRight, QueryDataKind.JoinRight>
    {
        internal static readonly QueryResultElement.Deserializer<LeftJoinResult<TLeft, TRight>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TLeft>(),
                    context.Deserialize<TRight>());
        TLeft IQueryData<TLeft, QueryDataKind.JoinLeft>.Value => Left;
        TRight? IOptionalQueryData<TRight, QueryDataKind.JoinRight>.Value => Right;
    }

    public sealed class DataSource<TSource>
    {
        private static readonly StringWithQualityHeaderValue gzip = new("gzip");

        private readonly PropertyColumnNameMapping.ImmutableBuilder columnNameMappings;
        private readonly SectionAtomicity.ImmutableBuilder? sectionAtomicity;
        private readonly Func<DataSourceOptions> getOptions;
        private readonly OnExecuteQuery? onExecute;
        private JsonSerializerOptions? serializerOptionsWithFormatting;
        private DataSourceOptions options => getOptions();

        internal DataSource(
            Func<DataSourceOptions> getOptions,
            OnExecuteQuery? onExecute,
            DataSourceJsonProvider getJsonRepresentation,
            PropertyColumnNameMapping.ImmutableBuilder columnNameMappings,
            SectionAtomicity.ImmutableBuilder? sectionAtomicity)
        {
            this.getOptions = getOptions;
            this.onExecute = onExecute;
            GetJsonRepresentation = getJsonRepresentation;
            this.columnNameMappings = columnNameMappings;
            this.sectionAtomicity = sectionAtomicity;
        }

        public readonly DataSourceJsonProvider GetJsonRepresentation;

        public JsonObject MapQueryToJson(IQueryWith.Source<TSource> query)
        {
            var mappings = query.ApplyPropertyColumnNameMappingChanges(columnNameMappings);
            var result = query.MapToJson(options.Serializer, mappings);
            result.Add("dataSource", GetJsonRepresentation());
            return result;
        }

        public async IAsyncEnumerable<TResult> ExecuteQuery<TResult>(
            IQueryWith.Source<TSource>.AndResult<TResult> query,
            bool onTruncatedResultsQueryRemaining = true,
            [EnumeratorCancellation] CancellationToken token = default)
        {
            if (onExecute is not null)
            {
                query = query.Copy();
                onExecute(query);
            }

            var queryForRemaining = new Mutable<IQueryWith.Source<TSource>> { Value = query };
            var atomicity = SectionAtomicity.ImmutableBuilder.Combine(query.SectionAtomicity, sectionAtomicity);
            var mappings = query.ApplyPropertyColumnNameMappingChanges(columnNameMappings);
            var deserializer = query;
            var truncatedResultHandler = query;
            byte[]? buffer = null;
            var context = new TruncatedQueryResultHandlingContext();
            async IAsyncEnumerable<TResult> Deserialize(Stream utf8Json, [EnumeratorCancellation] CancellationToken token)
            {
                buffer ??= ArrayPool<byte>.Shared.Rent(options.Serializer.DefaultBufferSize);
                var read = await utf8Json.ReadAsync(buffer, token);
                var results = deserializer
                    .Deserialize(new(new(utf8Json, buffer, read), options.Serializer, atomicity, mappings), token);
                queryForRemaining.Value = null;
                if (onTruncatedResultsQueryRemaining)
                    results = truncatedResultHandler.OnTruncatedResultsSetQueryForRemaining(results, context, queryForRemaining, token);
                await foreach (var result in results)
                    yield return result;
            }

            try
            {
                while (queryForRemaining.Value != null)
                {
                    var results = ExecuteQuery(queryForRemaining.Value, Deserialize, token);
                    await foreach (var result in results)
                        yield return result;
                }
            }
            finally
            {
                if (buffer is not null)
                    ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private async IAsyncEnumerable<TResult> ExecuteQuery<TResult>(
            IQueryWith.Source<TSource> query,
            Func<Stream, CancellationToken, IAsyncEnumerable<TResult>> deserialize,
            [EnumeratorCancellation] CancellationToken token = default)
        {
            var json = MapQueryToJson(query);
            using var content = JsonContent.Create(json, options: options.Serializer);
            using var request = new HttpRequestMessage(HttpMethod.Post, "druid/v2")
            {
                Content = content,
                Headers = { AcceptEncoding = { gzip } }
            };
            using var client = options.HttpClientFactory();
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

            try
            {
                response.EnsureSuccessStatusCode();
            }
            catch (HttpRequestException exception)
            {
                serializerOptionsWithFormatting ??= new(options.Serializer) { WriteIndented = true };
                var responseContent = await response.Content.ReadAsStringAsync(token);
                exception.Data.Add("requestContent", JsonSerializer.Serialize(json, serializerOptionsWithFormatting));
                exception.Data.Add(nameof(responseContent), responseContent);
                throw;
            }

            using var raw = await response.Content.ReadAsStreamAsync(token);
            var isGzip = response.Content.Headers.ContentEncoding.Contains(gzip.Value);
            using var decompressed = isGzip ? new GZipStream(raw, CompressionMode.Decompress) : raw;
            var results = deserialize(decompressed, token);
            await foreach (var result in results)
                yield return result;
        }

        public DataSource<TResult> ToQueryDataSource<TResult>(IQueryWith.Source<TSource>.AndResult<TResult> query) => new(
            getOptions,
            onExecute,
            () => new JsonObject
            {
                ["type"] = "query",
                ["query"] = MapQueryToJson(query)
            },
            columnNameMappings.Add<TResult>(),
            query.SectionAtomicity);

        public DataSource<InnerJoinData<TSource, TRight>> InnerJoin<TRight>(
            DataSource<TRight> right,
            Expression<QueryElementFactory<InnerJoinData<TSource, TRight>>.DruidExpression> condition,
            string rightPrefix = "r.")
            => Join<TRight, InnerJoinData<TSource, TRight>, InnerJoinData<TSource, TRight>>(right, rightPrefix, condition, "INNER");

        public DataSource<LeftJoinResult<TSource, TRight>> LeftJoin<TRight>(
            DataSource<TRight> right,
            Expression<QueryElementFactory<LeftJoinData<TSource, TRight>>.DruidExpression> condition,
            string rightPrefix = "r.")
            => Join<TRight, LeftJoinData<TSource, TRight>, LeftJoinResult<TSource, TRight>>(right, rightPrefix, condition, "LEFT");

        private DataSource<TResult> Join<TRight, TData, TResult>(
            DataSource<TRight> right, string rightPrefix, Expression<QueryElementFactory<TData>.DruidExpression> condition, string joinType)
        {
            var mappings = columnNameMappings
                .Combine(right.columnNameMappings)
                .Update<TRight>(mapping => mapping with { ColumnName = rightPrefix + mapping.ColumnName })
                .Add<TResult>();
            return new(
                getOptions,
                onExecute,
                () => new JsonObject
                {
                    ["type"] = "join",
                    ["left"] = GetJsonRepresentation(),
                    ["right"] = right.GetJsonRepresentation(),
                    [nameof(rightPrefix)] = rightPrefix,
                    [nameof(condition)] = DruidExpression.Map(condition, mappings).Expression,
                    [nameof(joinType)] = joinType
                },
                mappings,
                SectionAtomicity.ImmutableBuilder.Combine(
                    sectionAtomicity,
                    right.sectionAtomicity?.Update(atomicity => atomicity.WithColumnNameIfAtomic(rightPrefix + atomicity.ColumnNameIfAtomic))));
        }

        public DataSource<TSource> Union(DataSource<TSource> second)
            => new(
                getOptions,
                onExecute,
                () => new JsonObject
                {
                    ["type"] = "union",
                    ["dataSources"] = new JsonArray
                    {
                        GetJsonRepresentation(),
                        second.GetJsonRepresentation()
                    }
                },
                columnNameMappings,
                sectionAtomicity);

        public DataSource<Union<TSource, TSecond>> Union<TSecond>(DataSource<TSecond> second)
            => new(
                getOptions,
                onExecute,
                () => new JsonObject
                {
                    ["type"] = "union",
                    ["dataSources"] = new JsonArray
                    {
                        GetJsonRepresentation(),
                        second.GetJsonRepresentation()
                    }
                },
                columnNameMappings.Combine(second.columnNameMappings),
                SectionAtomicity.ImmutableBuilder.Combine(sectionAtomicity, second.sectionAtomicity));

        public DataSource<Union<TSource, TSecond, TThird>> Union<TSecond, TThird>(DataSource<TSecond> second, DataSource<TThird> third)
            => new(
                getOptions,
                onExecute,
                () => new JsonObject
                {
                    ["type"] = "union",
                    ["dataSources"] = new JsonArray
                    {
                        GetJsonRepresentation(),
                        second.GetJsonRepresentation(),
                        third.GetJsonRepresentation()
                    }
                },
                columnNameMappings.Combine(second.columnNameMappings).Combine(third.columnNameMappings),
                SectionAtomicity.ImmutableBuilder.Combine(sectionAtomicity, second.sectionAtomicity, third.sectionAtomicity));

        internal string? JsonRepresentationDebugView => GetJsonRepresentation()?.ToJsonString(options.Serializer);
    }
}
