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
    internal sealed record QueryResultDeserializerContext(
        JsonStreamReader Json,
        JsonSerializerOptions Options,
        SectionAtomicity.IProvider Atomicity,
        IColumnNameMappingProvider ColumnNameMappings);

    internal sealed class Mutable<TValue>
    {
        public TValue? Value { get; set; }
    }

    public interface IQueryResultDeserializer<TResult>
    {
        internal IAsyncEnumerable<TResult> Deserialize(QueryResultDeserializerContext context, CancellationToken token);
    }

    public interface IQueryWithSource<TSource> : IQueryWithInternal.SectionFactoryExpression_Atomicity
    {
        public interface AndResult<TResult> : IQueryWithSource<TSource>
        {
            public interface AndDeserializationAndTruncatedResultHandling<TContext> : AndResult<TResult>, IQueryResultDeserializer<TResult>
                where TContext : new()
            {
                internal IAsyncEnumerable<TResult> OnTruncatedResultsSetQueryForRemaining(
                    IAsyncEnumerable<TResult> results, TContext context, Mutable<IQueryWithSource<TSource>> setter, CancellationToken token);
            }
        }
    }

    public class TruncatedResultsException : Exception
    {
        public TruncatedResultsException(string? message = null, Exception? inner = null) : base(message ?? "Druid query returned truncated results.", inner)
        {
        }
    }

    public delegate JsonNode? DataSourceJsonProvider();

    public readonly record struct Union<TFirst, TSecond>(TFirst? First, TSecond? Second)
    {
        internal static readonly QueryResultElement.Deserializer<Union<TFirst, TSecond>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TFirst>(),
                    context.Deserialize<TSecond>());
    }

    public readonly record struct Union<TFirst, TSecond, TThird>(TFirst? First, TSecond? Second, TThird? Third)
    {
        internal static readonly QueryResultElement.Deserializer<Union<TFirst, TSecond, TThird>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TFirst>(),
                    context.Deserialize<TSecond>(),
                    context.Deserialize<TThird>());
    }

    public readonly record struct InnerJoinData<TLeft, TRight>(TLeft Left, TRight Right)
    {
        internal static readonly QueryResultElement.Deserializer<InnerJoinData<TLeft, TRight>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TLeft>(),
                    context.Deserialize<TRight>());
    }

    public readonly record struct LeftJoinData<TLeft, TRight>(TLeft Left, TRight Right);
    public readonly record struct LeftJoinResult<TLeft, TRight>(TLeft Left, TRight? Right)
    {
        internal static readonly QueryResultElement.Deserializer<LeftJoinResult<TLeft, TRight>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TLeft>(),
                    context.Deserialize<TRight>());
    }

    public sealed class DataSource<TSource>
    {
        private static readonly StringWithQualityHeaderValue gzip = new("gzip");

        private readonly IColumnNameMappingProvider.ImmutableBuilder columnNameMappings;
        private readonly SectionAtomicity.IProvider.ImmutableBuilder? sectionAtomicity;
        private readonly Func<DataSourceOptions> getOptions;
        private JsonSerializerOptions? serializerOptionsWithFormatting;
        private DataSourceOptions options => getOptions();

        internal DataSource(
            Func<DataSourceOptions> getOptions,
            DataSourceJsonProvider getJsonRepresentation,
            IColumnNameMappingProvider.ImmutableBuilder columnNameMappings,
            SectionAtomicity.IProvider.ImmutableBuilder? sectionAtomicity = null)
        {
            this.getOptions = getOptions;
            GetJsonRepresentation = getJsonRepresentation;
            this.columnNameMappings = columnNameMappings;
            this.sectionAtomicity = sectionAtomicity;
        }

        public readonly DataSourceJsonProvider GetJsonRepresentation;

        public JsonObject MapQueryToJson(IQueryWithSource<TSource> query)
        {
            var result = query.MapToJson(options.Serializer, columnNameMappings);
            result.Add("dataSource", GetJsonRepresentation());
            return result;
        }

        public async IAsyncEnumerable<TResult> ExecuteQuery<TResult, TContext>(
            IQueryWithSource<TSource>.AndResult<TResult>.AndDeserializationAndTruncatedResultHandling<TContext> query,
            bool onTruncatedResultsQueryRemaining = true,
            [EnumeratorCancellation] CancellationToken token = default)
            where TContext : new()
        {
            var queryForRemaining = new Mutable<IQueryWithSource<TSource>> { Value = query };
            var atomicity = SectionAtomicity.IProvider.ImmutableBuilder.Combine(query.SectionAtomicity, sectionAtomicity);
            var deserializer = query;
            var truncatedResultHandler = query;
            byte[]? buffer = null;
            var context = new TContext();
            async IAsyncEnumerable<TResult> Deserialize(Stream utf8Json, [EnumeratorCancellation] CancellationToken token)
            {
                buffer ??= ArrayPool<byte>.Shared.Rent(options.Serializer.DefaultBufferSize);
                var read = await utf8Json.ReadAsync(buffer, token);
                var results = deserializer
                    .Deserialize(new(new(utf8Json, buffer, read), options.Serializer, atomicity, columnNameMappings), token)
                    .Catch<TResult, UnexpectedEndOfStreamException>(exception => throw new TruncatedResultsException(inner: exception), token);
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

        public async IAsyncEnumerable<TResult> ExecuteQuery<TResult>(
            IQueryWithSource<TSource> query,
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

        public DataSource<TResult> ToQueryDataSource<TResult>(IQueryWithSource<TSource>.AndResult<TResult> query) => new(
            getOptions,
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
                .Update<TRight>(mapping => mapping with { ColumnName = rightPrefix + mapping.ColumnName });
            return new(
                getOptions,
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
                SectionAtomicity.IProvider.ImmutableBuilder.Combine(
                    sectionAtomicity, 
                    right.sectionAtomicity?.Update(atomicity => atomicity.WithColumnNameIfAtomic(rightPrefix + atomicity.ColumnNameIfAtomic))));
        }

        public DataSource<Union<TSource, TSecond>> Union<TSecond>(DataSource<TSecond> second)
            => new(
                getOptions,
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
                SectionAtomicity.IProvider.ImmutableBuilder.Combine(sectionAtomicity, second.sectionAtomicity));

        public DataSource<Union<TSource, TSecond, TThird>> Union<TSecond, TThird>(DataSource<TSecond> second, DataSource<TThird> third)
            => new(
                getOptions,
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
                SectionAtomicity.IProvider.ImmutableBuilder.Combine(sectionAtomicity, second.sectionAtomicity, third.sectionAtomicity));

        internal string? JsonRepresentationDebugView => GetJsonRepresentation()?.ToJsonString(options.Serializer);
    }
}
