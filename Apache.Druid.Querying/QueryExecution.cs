using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;

namespace Apache.Druid.Querying
{
    public interface IQueryWithMappedResult<TResult> : IQuery
    {
        TResult Map(JsonElement from, JsonSerializerOptions serializerOptions);
    }

    public interface IQueryWithResult<TResult> : IQuery
    {
    }

    public interface IQueryExecutor
    {
        IAsyncEnumerable<TResult> Execute<TResult>(Dictionary<string, object?> query, CancellationToken token);
    }

    public interface IDataSource<TSource>
    {
        public class Implementation : IInitializer
        {
            private JsonSerializerOptions? serializerOptionsWithFormatting;
            IDataSource<TSource>.Context? IDataSource<TSource>.IInitializer.context { get; set; }
            private Context Context => (this as IInitializer).Context;

            public IAsyncEnumerable<TResult> ExecuteQuery<TResult>(IQueryWithResult<TResult> query, CancellationToken token = default)
                => Execute<TResult>(query, token);

            public async IAsyncEnumerable<TResult> ExecuteQuery<TResult>(IQueryWithMappedResult<TResult> query, [EnumeratorCancellation] CancellationToken token = default)
            {
                var json = Execute<JsonElement>(query, token);
                await foreach (var result in json)
                    yield return query.Map(result, Context.SerializerOptions);
            }

            private async IAsyncEnumerable<TResult> Execute<TResult>(IQuery query, [EnumeratorCancellation] CancellationToken token = default)
            {
                var (id, serializerOptions, clientFactory) = Context;
                serializerOptionsWithFormatting ??= new(serializerOptions) { WriteIndented = true };
                var asDictionary = query
                    .GetState()
                    .ToDictionary(pair => pair.Key, pair => pair.Value.Value);
                asDictionary.Add("dataSource", id);
                using var content = JsonContent.Create(query, options: serializerOptions);
                using var request = new HttpRequestMessage(HttpMethod.Post, "druid/v2") { Content = content };
                using var client = clientFactory();
                using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

                try
                {
                    response.EnsureSuccessStatusCode();
                }
                catch (HttpRequestException exception)
                {
                    var responseContent = await response.Content.ReadAsStringAsync(token);
                    exception.Data.Add("requestContent", JsonSerializer.Serialize(query, serializerOptionsWithFormatting));
                    exception.Data.Add(nameof(responseContent), responseContent);
                    throw;
                }

                using var stream = await response.Content.ReadAsStreamAsync(token);
                var results = JsonSerializer.DeserializeAsyncEnumerable<TResult>(stream, serializerOptions, token);
                await foreach (var result in results)
                    yield return result!;
            }
        }

        public interface IInitializer
        {
            [SuppressMessage("Style", "IDE1006:Naming Styles")]
            private protected Context? context { get; set; }

            internal Context Context => context ??
                throw new InvalidOperationException($"Attempted to use an uninitialized instance of {typeof(IDataSource<TSource>)}.");

            public void Initialize(Context context)
            {
                if (this.context is not null)
                    throw new InvalidOperationException("Already initialized.");
                this.context = context;
            }
        }

        public sealed record Context(
            string Id,
            JsonSerializerOptions SerializerOptions,
            Func<HttpClient> HttpClientFactory);
    }
}
