using System;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using System.Text.Json;

namespace Apache.Druid.Querying.DependencyInjection
{
    public sealed record DataSourceInitlializationState(
        string Id,
        JsonSerializerOptions SerializerOptions,
        Func<HttpClient> HttpClientFactory);

    public interface IDataSourceInitializer<TSource>
    {
        [SuppressMessage("Style", "IDE1006:Naming Styles")]
        private protected DataSourceInitlializationState? state { get; set; }

        internal DataSourceInitlializationState State => state ??
            throw new InvalidOperationException($"Attempted to use an uninitialized instance of {typeof(DataSource<TSource>)}.");

        public void Initialize(DataSourceInitlializationState state)
        {
            if (this.state is not null)
                throw new InvalidOperationException("Already initialized.");
            this.state = state;
        }
    }
}
