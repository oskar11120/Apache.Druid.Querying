using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Json;
using Apache.Druid.Querying.Internal.Sections;
using Apache.Druid.Querying.Json;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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

    internal sealed class TruncatedResultHandlingContext<TSource>
    {
        public readonly PropertyColumnNameMapping.IProvider Mappings;
        public readonly TruncatedResultHandlingContextState State = new();
        public IQueryWith.Source<TSource>? NextQuerySetter;

        public TruncatedResultHandlingContext(PropertyColumnNameMapping.IProvider mappings)
            => Mappings = mappings;
    }

    internal sealed class TruncatedResultHandlingContextState
    {
        private readonly Dictionary<Type, object> data = new();
        public bool TryGet<TState>([MaybeNullWhen(false)] out TState state)
        {
            if (data.TryGetValue(typeof(TState), out var existing))
            {
                state = (TState)existing;
                return true;
            }

            state = default;
            return false;
        }

        public void Add<TState>(TState state)
            where TState : notnull
            => data.Add(typeof(TState), state);

        public TState GetOrAdd<TState>()
            where TState : notnull, new()
        {
            if (TryGet<TState>(out var existing))
                return existing;
            var @new = new TState();
            Add(@new);
            return @new;
        }
    }

    public static partial class IQueryWith
    {
        public interface Result<out TResult>
        {
            internal IAsyncEnumerable<TResult> Deserialize(QueryResultDeserializationContext context, CancellationToken token);
        }

        public interface Source<out TSource> : IQueryWithInternal.SectionAtomicity, IQueryWithInternal.PropertyColumnNameMappingChanges
        {
        }

        public interface SourceAndResult<TSource, TResult> : Source<TSource>, Result<TResult>
        {
            internal IAsyncEnumerable<TResult> OnTruncatedResultsSetQueryForRemaining(
                IAsyncEnumerable<TResult> currentQueryResults, TruncatedResultHandlingContext<TSource> context, CancellationToken token);
        }
    }

    public delegate JsonNode? DataSourceJsonProvider();

    public partial record QueryDataKind
    {
        public sealed record Source : QueryDataKind;

        public sealed record UnionFirst : QueryDataKind;
        public sealed record UnionSecond : QueryDataKind;
        public sealed record UnionThird : QueryDataKind;

        public sealed record JoinLeft : QueryDataKind;
        public sealed record JoinRight : QueryDataKind;
    }

    public interface IOptionalQueryData<out TValue, out TKind> where TKind : QueryDataKind
    {
        TValue? Value { get; }
    }

    public interface IQueryData<out TValue, out TKind> : IOptionalQueryData<TValue, TKind> where TKind : QueryDataKind
    {
        new TValue Value { get; }
        TValue IOptionalQueryData<TValue, TKind>.Value => Value;
    }

    public sealed record Union<TFirst, TSecond>(TFirst? First, TSecond? Second) :
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

    public sealed record Union<TFirst, TSecond, TThird>(TFirst? First, TSecond? Second, TThird? Third) :
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

    public sealed record InnerJoinData<TLeft, TRight>(TLeft Left, TRight Right) :
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

    public sealed record LeftJoinData<TLeft, TRight>(TLeft Left, TRight Right) :
        IQueryData<TLeft, QueryDataKind.JoinLeft>,
        IQueryData<TRight, QueryDataKind.JoinRight>
    {
        TLeft IQueryData<TLeft, QueryDataKind.JoinLeft>.Value => Left;
        TRight IQueryData<TRight, QueryDataKind.JoinRight>.Value => Right;
    }

    public sealed record LeftJoinResult<TLeft, TRight>(TLeft Left, TRight? Right) :
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

    internal sealed record DataSourceContext(
        Lazy<DataSourceOptions> Options,
        OnMapQueryToJson OnMap,
        DataSourceJsonProvider GetJsonRepresentation,
        PropertyColumnNameMapping.ImmutableBuilder ColumnNameMappings,
        SectionAtomicity.ImmutableBuilder? SectionAtomicity)
    {
        public JsonSerializerOptions QuerySerializerOptions => Options.Value.QuerySerializer;
        public JsonSerializerOptions DataSerializerOptions => Options.Value.DataSerializer;
        public Func<HttpClient> HttpClientFactory => Options.Value.HttpClientFactory;

        private JsonSerializerOptions? querySerializerOptionsIndented;
        public JsonSerializerOptions QuerySerializerOptionsIndented => querySerializerOptionsIndented ??=
            QuerySerializerOptions.WriteIndented ? QuerySerializerOptions : new(QuerySerializerOptions) { WriteIndented = true };
    }

    public class DataSource<TSource>
    {
        private static readonly StringWithQualityHeaderValue gzip = new("gzip");

        private DataSourceContext? context;

        private DataSourceContext Context => context ??
            throw new InvalidOperationException($"Attempted to use an uninitialized instance of {GetType()}.");

        internal void Initialize(DataSourceContext context)
            => this.context = context;

        public virtual JsonObject MapQueryToJson(IQueryWith.Source<TSource> query)
        {
            var mappings = query.ApplyPropertyColumnNameMappingChanges(Context.ColumnNameMappings);
            var mapContext = new QueryToJsonMappingContext(Context.QuerySerializerOptions, Context.DataSerializerOptions, mappings);
            var result = query.MapToJson(mapContext);
            result.Add("dataSource", Context.GetJsonRepresentation());
            Context.OnMap(query, result);
            return result;
        }

        public virtual async IAsyncEnumerable<TResult> ExecuteQuery<TResult>(
            IQueryWith.SourceAndResult<TSource, TResult> query,
            bool onTruncatedResultsQueryRemaining = true,
            [EnumeratorCancellation] CancellationToken token = default)
        {
            var atomicity = SectionAtomicity.ImmutableBuilder.Combine(query.SectionAtomicity, Context.SectionAtomicity);
            var mappings = query.ApplyPropertyColumnNameMappingChanges(Context.ColumnNameMappings);
            var deserializer = query;
            var truncatedResultHandler = query;
            var truncatedResultHandlingContext = new TruncatedResultHandlingContext<TSource>(mappings);
            byte[]? buffer = null;

            async IAsyncEnumerable<TResult> Deserialize(Stream utf8Json, [EnumeratorCancellation] CancellationToken token)
            {
                buffer ??= ArrayPool<byte>.Shared.Rent(Context.DataSerializerOptions.DefaultBufferSize);
                var read = await utf8Json.ReadAsync(buffer, token);
                var streamReader = new JsonStreamReader(utf8Json, buffer, read);
                var results = deserializer.Deserialize(new(streamReader, Context.DataSerializerOptions, atomicity, mappings), token);
                truncatedResultHandlingContext.NextQuerySetter = null;
                if (onTruncatedResultsQueryRemaining)
                {
                    results = query.OnTruncatedResultsSetQueryForRemaining(results, truncatedResultHandlingContext, token);
                    await foreach (var result in results)
                        yield return result;
                }
                else
                {
                    await foreach (var result in results)
                        yield return result;
                }
            }

            try
            {
                truncatedResultHandlingContext.NextQuerySetter = query;
                while (truncatedResultHandlingContext.NextQuerySetter is IQueryWith.Source<TSource> nextQuery)
                {
                    var results = ExecuteQuery(nextQuery, Deserialize, token);
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
            using var content = JsonContent.Create(json, options: Context.QuerySerializerOptions);
            using var request = new HttpRequestMessage(HttpMethod.Post, Context.Options.Value.NativeQueryEndpointUri)
            {
                Content = content,
                Headers = { AcceptEncoding = { gzip } }
            };
            using var client = Context.HttpClientFactory();
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

            try
            {
                response.EnsureSuccessStatusCode();
            }
            catch (HttpRequestException exception)
            {
                var responseContent = await response.Content.ReadAsStringAsync(token);
                exception.Data.Add("requestContent", JsonSerializer.Serialize(json, Context.QuerySerializerOptionsIndented));
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

        public virtual DataSource<TResult> ToQueryDataSource<TResult>(IQueryWith.SourceAndResult<TSource, TResult> query)
            => New<TResult>(
                () => new JsonObject
                {
                    ["type"] = "query",
                    ["query"] = MapQueryToJson(query)
                },
                Context.ColumnNameMappings.Add<TResult>(),
                query.SectionAtomicity);

        public virtual DataSource<InnerJoinData<TSource, TRight>> InnerJoin<TRight>(
            DataSource<TRight> right,
            Expression<QueryElementFactory<InnerJoinData<TSource, TRight>>.DruidExpression> condition,
            string rightPrefix = "r.")
            => Join<TRight, InnerJoinData<TSource, TRight>, InnerJoinData<TSource, TRight>>(right, rightPrefix, condition, "INNER");

        public virtual DataSource<LeftJoinResult<TSource, TRight>> LeftJoin<TRight>(
            DataSource<TRight> right,
            Expression<QueryElementFactory<LeftJoinData<TSource, TRight>>.DruidExpression> condition,
            string rightPrefix = "r.")
            => Join<TRight, LeftJoinData<TSource, TRight>, LeftJoinResult<TSource, TRight>>(right, rightPrefix, condition, "LEFT");

        private DataSource<TResult> Join<TRight, TData, TResult>(
            DataSource<TRight> right, string rightPrefix, Expression<QueryElementFactory<TData>.DruidExpression> condition, string joinType)
        {
            var mappings = Context.ColumnNameMappings
                .Combine(right.Context.ColumnNameMappings)
                .Update<TRight>(mapping => mapping with { ColumnName = rightPrefix + mapping.ColumnName })
                .Add<TResult>();
            return New<TResult>(
                () => new JsonObject
                {
                    ["type"] = "join",
                    ["left"] = Context.GetJsonRepresentation(),
                    ["right"] = right.Context.GetJsonRepresentation(),
                    [nameof(rightPrefix)] = rightPrefix,
                    [nameof(condition)] = DruidExpression.Map(condition, mappings, Context.DataSerializerOptions).Expression,
                    [nameof(joinType)] = joinType
                },
                mappings,
                SectionAtomicity.ImmutableBuilder.Combine(
                    Context.SectionAtomicity,
                    right.Context.SectionAtomicity?.Update(atomicity => atomicity.WithColumnNameIfAtomic(rightPrefix + atomicity.ColumnNameIfAtomic))));
        }

        public virtual DataSource<TSource> Union(DataSource<TSource> second)
            => New<TSource>(
                () => new JsonObject
                {
                    ["type"] = "union",
                    ["dataSources"] = new JsonArray
                    {
                        Context.GetJsonRepresentation(),
                        second.Context.GetJsonRepresentation()
                    }
                },
                Context.ColumnNameMappings,
                Context.SectionAtomicity);

        public virtual DataSource<Union<TSource, TSecond>> Union<TSecond>(DataSource<TSecond> second)
            => New<Union<TSource, TSecond>>(
                () => new JsonObject
                {
                    ["type"] = "union",
                    ["dataSources"] = new JsonArray
                    {
                        Context.GetJsonRepresentation(),
                        second.Context.GetJsonRepresentation()
                    }
                },
                Context.ColumnNameMappings.Combine(second.Context.ColumnNameMappings),
                SectionAtomicity.ImmutableBuilder.Combine(Context.SectionAtomicity, second.Context.SectionAtomicity));

        public virtual DataSource<Union<TSource, TSecond, TThird>> Union<TSecond, TThird>(DataSource<TSecond> second, DataSource<TThird> third)
            => New<Union<TSource, TSecond, TThird>>(
                () => new JsonObject
                {
                    ["type"] = "union",
                    ["dataSources"] = new JsonArray
                    {
                        Context.GetJsonRepresentation(),
                        second.Context.GetJsonRepresentation(),
                        third.Context.GetJsonRepresentation()
                    }
                },
                Context.ColumnNameMappings.Combine(second.Context.ColumnNameMappings).Combine(third.Context.ColumnNameMappings),
                SectionAtomicity.ImmutableBuilder.Combine(Context.SectionAtomicity, second.Context.SectionAtomicity, third.Context.SectionAtomicity));

        internal string? JsonRepresentationDebugView => Context.GetJsonRepresentation()?.ToJsonString(Context.QuerySerializerOptions);

        private DataSource<NewTSource> New<NewTSource>(
            DataSourceJsonProvider GetJsonRepresentation,
            PropertyColumnNameMapping.ImmutableBuilder ColumnNameMappings,
            SectionAtomicity.ImmutableBuilder? SectionAtomicity)
        {
            var @new = new DataSource<NewTSource>();
            @new.Initialize(Context with
            {
                GetJsonRepresentation = GetJsonRepresentation,
                ColumnNameMappings = ColumnNameMappings,
                SectionAtomicity = SectionAtomicity
            });
            return @new;
        }
    }
}
