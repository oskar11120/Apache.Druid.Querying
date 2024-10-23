using System;
using System.Net.Http;
using System.Text.Json;

namespace Apache.Druid.Querying.DependencyInjection
{
    public sealed class DataSourceOptions
    {
        public DataSourceOptions(
            JsonSerializerOptions querySerializer, 
            JsonSerializerOptions dataSerializer, 
            Func<HttpClient> httpClientFactory,
            Func<string>? getNativeQueryEndpointUri = null)
        {
            QuerySerializer = querySerializer;
            DataSerializer = dataSerializer;
            HttpClientFactory = httpClientFactory;
            this.getNativeQueryEndpointUri = getNativeQueryEndpointUri ?? (static () => "druid/v2");
        }

        public JsonSerializerOptions QuerySerializer { get; }
        public JsonSerializerOptions DataSerializer { get; }
        public Func<HttpClient> HttpClientFactory {  get; }

        private readonly Func<string> getNativeQueryEndpointUri;
        public string NativeQueryEndpointUri => getNativeQueryEndpointUri();
    };

    public interface IDataSourceInitializer
    {
        private protected DataSourceOptions? options { get; set; }

        internal DataSourceOptions Options => options ??
            throw new InvalidOperationException($"Attempted to use an uninitialized instance of {GetType()}.");

        bool Initialized => options is not null;

        void Initialize(DataSourceOptions options)
        {
            if (this.options is not null)
                throw new InvalidOperationException("Already initialized.");
            this.options = options;
        }
    }
}
