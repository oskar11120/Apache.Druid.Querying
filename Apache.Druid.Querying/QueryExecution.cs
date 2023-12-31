﻿using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;

namespace Apache.Druid.Querying
{
    public interface IQueryResultMapper<TResult>
    {
        internal IAsyncEnumerable<TResult> Map(JsonStreamReader json, JsonSerializerOptions options, CancellationToken token);
    }

    public interface IQueryWithMappedResult<TResult, TMapper> : IQuery where TMapper : IQueryResultMapper<TResult>, new()
    {
    }

    public interface IQueryWithResult<TResult> : IQuery
    {
    }

    public class DataSource<TSource> : IDataSourceInitializer<TSource>
    {
        private static readonly StringWithQualityHeaderValue gzip = new("gzip");

        private JsonSerializerOptions? serializerOptionsWithFormatting;
        DataSourceInitlializationState? IDataSourceInitializer<TSource>.state { get; set; }
        private DataSourceInitlializationState State => (this as IDataSourceInitializer<TSource>).State;

        public virtual IAsyncEnumerable<TResult> ExecuteQuery<TResult>(IQuery query, CancellationToken token = default)
            => Execute<TResult>(query, token);

        public virtual IAsyncEnumerable<TResult> ExecuteQuery<TResult>(IQueryWithResult<TResult> query, CancellationToken token = default)
            => Execute<TResult>(query, token);

        public virtual IAsyncEnumerable<TResult> ExecuteQuery<TResult, TMapper>(
            IQueryWithMappedResult<TResult, TMapper> query, CancellationToken token = default)
            where TMapper : IQueryResultMapper<TResult>, new()
        {
            static async IAsyncEnumerable<TResult> Deserialize(Stream stream, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                var mapper = new TMapper();
                var buffer = ArrayPool<byte>.Shared.Rent(JsonStreamReader.Size);

                try
                {
                    var read = await stream.ReadAsync(buffer, token);
                    var results = mapper.Map(new(stream, buffer, read), options, token);
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
        private IAsyncEnumerable<TResult> Execute<TResult>(IQuery query, CancellationToken token = default)
            => Execute<TResult>(query, JsonSerializer.DeserializeAsyncEnumerable<TResult>, token);
#pragma warning restore CS8621 // Nullability of reference types in return type doesn't match the target delegate (possibly because of nullability attributes).

        private async IAsyncEnumerable<TResult> Execute<TResult>(
            IQuery query,
            Func<Stream, JsonSerializerOptions, CancellationToken, IAsyncEnumerable<TResult>> deserialize,
            [EnumeratorCancellation] CancellationToken token = default)
        {
            var (id, serializerOptions, clientFactory) = State;
            serializerOptionsWithFormatting ??= new(serializerOptions) { WriteIndented = true };
            var asDictionary = query
                .GetState()
                .ToDictionary(
                    pair => pair.Key,
                    pair => pair.Value(serializerOptions, IArgumentColumnNameProvider.Implementation<TSource>.Singleton));
            asDictionary.Add("dataSource", id!);
            using var content = JsonContent.Create(asDictionary, options: serializerOptions);
            using var request = new HttpRequestMessage(HttpMethod.Post, "druid/v2")
            {
                Content = content,
                Headers = { AcceptEncoding = { gzip } }
            };
            using var client = clientFactory();
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

            try
            {
                response.EnsureSuccessStatusCode();
            }
            catch (HttpRequestException exception)
            {
                var responseContent = await response.Content.ReadAsStringAsync(token);
                exception.Data.Add("requestContent", JsonSerializer.Serialize(asDictionary, serializerOptionsWithFormatting));
                exception.Data.Add(nameof(responseContent), responseContent);
                throw;
            }

            using var raw = await response.Content.ReadAsStreamAsync(token);
            var isGzip = response.Content.Headers.ContentEncoding.Contains(gzip.Value);
            using var decompressed = isGzip ? new GZipStream(raw, CompressionMode.Decompress) : raw;
            var results = deserialize(decompressed, serializerOptions, token);
            await foreach (var result in results)
                yield return result!;
        }
    }
}
