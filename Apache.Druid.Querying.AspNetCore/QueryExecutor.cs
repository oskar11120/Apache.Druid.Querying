using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Apache.Druid.Querying.AspNetCore
{
    internal sealed class QueryExecutor : IQueryExecutor
    {
        private readonly IHttpClientFactory clientFactory;
        private readonly JsonSerializerOptions serializerOptions;
        private readonly JsonSerializerOptions serializerOptionsWithFormatting;
        private readonly string clientId;

        public QueryExecutor(IHttpClientFactory clientFactory, JsonSerializerOptions serilizerOptions, string clientId)
        {
            this.clientFactory = clientFactory;
            this.serializerOptions = serilizerOptions;
            serializerOptionsWithFormatting = new(serializerOptions) { WriteIndented = true };
            this.clientId = clientId;
        }

        public async IAsyncEnumerable<TResult> Execute<TResult>(Dictionary<string, object?> query, [EnumeratorCancellation] CancellationToken token)
        {
            
        }
    }
}
