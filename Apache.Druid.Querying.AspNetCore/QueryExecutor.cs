using System.IO;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Apache.Druid.Querying.AspNetCore
{
    internal sealed class QueryExecutor : IQueryExecutor
    {
        private readonly IHttpClientFactory clientFactory;
        private readonly JsonSerializerOptions serilizerOptions;
        private readonly string clientId;

        public QueryExecutor(IHttpClientFactory clientFactory, JsonSerializerOptions serilizerOptions, string clientId)
        {
            this.clientFactory = clientFactory;
            this.serilizerOptions = serilizerOptions;
            this.clientId = clientId;
        }

        public async IAsyncEnumerable<TResult> Execute<TResult>(Dictionary<string, object?> query, [EnumeratorCancellation] CancellationToken token)
        {
            using var content = JsonContent.Create(query, options: serilizerOptions);
            using var request = new HttpRequestMessage(HttpMethod.Post, "druid/v2") { Content = content };
            using var client = clientFactory.CreateClient(clientId);
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);

            try
            {
                response.EnsureSuccessStatusCode();
            }
            catch (HttpRequestException exception)
            {
                var responseContent = await response.Content.ReadAsStringAsync(token);
                exception.Data.Add(nameof(responseContent), responseContent);
                throw;
            }

            using var stream = await response.Content.ReadAsStreamAsync(token);
            var results = JsonSerializer.DeserializeAsyncEnumerable<TResult>(stream, serilizerOptions, token);
            await foreach (var result in results)
                yield return result!;
        }
    }
}
