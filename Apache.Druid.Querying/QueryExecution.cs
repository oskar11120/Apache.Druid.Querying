using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Sections;
using Apache.Druid.Querying.Json;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;

namespace Apache.Druid.Querying
{
    internal readonly record struct QueryResultMapperContext(
        JsonStreamReader Json,
        JsonSerializerOptions Options,
        SectionAtomicity.IProvider Atomicity,
        IColumnNameMappingProvider ColumnNameMappings);

    public interface IQueryResultMapper<TResult>
    {
        internal IAsyncEnumerable<TResult> Map(QueryResultMapperContext context, CancellationToken token);
    }

    public interface IQueryWithSource<TSource> : IQuery
    {
        public interface AndResult<TResult> : IQueryWithSource<TSource>
        {
        }

        public interface AndMappedResult<TResult, TMapper> :
            IQueryWithSource<TSource>
            where TMapper : IQueryResultMapper<TResult>, new()
        {
        }
    }

    public delegate JsonNode? DataSourceJsonProvider();

    public readonly record struct InnerJoinResult<TLeft, TRight>(TLeft Left, TRight Right)
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<InnerJoinResult<TLeft, TRight>>
        {
            public InnerJoinResult<TLeft, TRight> Deserialize(ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TLeft>(),
                    context.Deserialize<TRight>());
        }
    }

    public readonly record struct LeftJoinResult<TLeft, TRight>(TLeft Left, TRight? Right)
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<LeftJoinResult<TLeft, TRight>>
        {
            public LeftJoinResult<TLeft, TRight> Deserialize(ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TLeft>(),
                    context.Deserialize<TRight>());
        }
    }

    public sealed class DataSource<TSource>
    {
        private static readonly StringWithQualityHeaderValue gzip = new("gzip");

        private readonly Func<DataSourceOptions> getOptions;
        private JsonSerializerOptions? serializerOptionsWithFormatting;
        private DataSourceOptions options => getOptions();

        internal DataSource(
            Func<DataSourceOptions> getOptions,
            DataSourceJsonProvider getJsonRepresentation,
            IColumnNameMappingProvider.ImmutableBuilder columnNameMappings)
        {
            this.getOptions = getOptions;
            GetJsonRepresentation = getJsonRepresentation;
            ColumnNameMappings = columnNameMappings;
        }

        public readonly DataSourceJsonProvider GetJsonRepresentation;
        internal readonly IColumnNameMappingProvider.ImmutableBuilder ColumnNameMappings;

        public JsonObject MapQueryToJson(IQueryWithSource<TSource> query)
        {
            var result = query.MapToJson(options.Serializer, ColumnNameMappings);
            result.Add("dataSource", GetJsonRepresentation());
            return result;
        }

        public IAsyncEnumerable<TResult> ExecuteQuery<TResult>(IQueryWithSource<TSource> query, CancellationToken token = default)
            => Execute<TResult>(query, token);

        public IAsyncEnumerable<TResult> ExecuteQuery<TResult>(IQueryWithSource<TSource>.AndResult<TResult> query, CancellationToken token = default)
            => Execute<TResult>(query, token);

        public IAsyncEnumerable<TResult> ExecuteQuery<TResult, TMapper>(
            IQueryWithSource<TSource>.AndMappedResult<TResult, TMapper> query, CancellationToken token = default)
            where TMapper : IQueryResultMapper<TResult>, new()
        {
            var atomicity = query.GetSectionAtomicity();
            async IAsyncEnumerable<TResult> Deserialize(Stream stream, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                var mapper = new TMapper();
                var buffer = ArrayPool<byte>.Shared.Rent(JsonStreamReader.Size);

                try
                {
                    var read = await stream.ReadAsync(buffer, token);
                    var results = mapper.Map(new(new(stream, buffer, read), options, atomicity, ColumnNameMappings), token);
                    await foreach (var result in results)
                        yield return result!;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }

            return Execute(query, Deserialize, token);
        }

#pragma warning disable CS8621 // Nullability of reference types in return type doesn't match the target delegate (possibly because of nullability attributes).
        private IAsyncEnumerable<TResult> Execute<TResult>(IQueryWithSource<TSource> query, CancellationToken token = default)
            => Execute<TResult>(query, JsonSerializer.DeserializeAsyncEnumerable<TResult>, token);
#pragma warning restore CS8621 // Nullability of reference types in return type doesn't match the target delegate (possibly because of nullability attributes).

        private async IAsyncEnumerable<TResult> Execute<TResult>(
            IQueryWithSource<TSource> query,
            Func<Stream, JsonSerializerOptions, CancellationToken, IAsyncEnumerable<TResult>> deserialize,
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
            var results = deserialize(decompressed, options.Serializer, token);
            await foreach (var result in results)
                yield return result!;
        }

        public DataSource<TResult> WrapQuery<TResult>(IQueryWithSource<TSource>.AndResult<TResult> query)
            => Wrap<TResult>(query);

        public DataSource<TResult> WrapQuery<TResult, TMapper>(IQueryWithSource<TSource>.AndMappedResult<TResult, TMapper> query)
            where TMapper : IQueryResultMapper<TResult>, new()
            => Wrap<TResult>(query);

        private DataSource<TResult> Wrap<TResult>(IQueryWithSource<TSource> query) => new(
            getOptions,
            () => new JsonObject
            {
                ["type"] = "query",
                ["query"] = MapQueryToJson(query)
            },
            ColumnNameMappings);

        public DataSource<InnerJoinResult<TSource, TRight>> InnerJoin<TRight>(DataSource<TRight> right, string rightPrefix, string condition)
            => Join<TRight, InnerJoinResult<TSource, TRight>>(right, rightPrefix, condition, "INNER");

        public DataSource<LeftJoinResult<TSource, TRight>> LeftJoin<TRight>(DataSource<TRight> right, string rightPrefix, string condition)
            => Join<TRight, LeftJoinResult<TSource, TRight>>(right, rightPrefix, condition, "LEFT");

        private DataSource<TResult> Join<TRight, TResult>(
            DataSource<TRight> right, string rightPrefix, string condition, string joinType) => new(
            getOptions,
            () => new JsonObject
            {
                ["type"] = "join",
                ["left"] = GetJsonRepresentation(),
                ["right"] = right.GetJsonRepresentation(),
                [nameof(rightPrefix)] = rightPrefix,
                [nameof(condition)] = condition,
                [nameof(joinType)] = joinType
            },
            ColumnNameMappings.Combine(right.ColumnNameMappings));

        internal string? JsonRepresentationDebugView => GetJsonRepresentation()?.ToJsonString(options.Serializer);
    }
}
