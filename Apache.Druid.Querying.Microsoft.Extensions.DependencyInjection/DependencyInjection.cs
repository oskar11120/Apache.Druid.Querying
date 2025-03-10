﻿using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;
using Apache.Druid.Querying.Json;
using Apache.Druid.Querying.DependencyInjection;

namespace Apache.Druid.Querying.Microsoft.Extensions.DependencyInjection
{
    public class DataSourceProviderBuilder
    {
        public DataSourceProviderBuilder(
            IHttpClientBuilder clientBuilder,
            JsonSerializerOptions querySerializerOptions,
            JsonSerializerOptions dataSerializerOptions)
        {
            ClientBuilder = clientBuilder;
            QuerySerializerOptions = querySerializerOptions;
            DataSerializerOptions = dataSerializerOptions;
            Services = clientBuilder.Services;
        }

        public IServiceCollection Services { get; }
        public IHttpClientBuilder ClientBuilder { get; }
        public JsonSerializerOptions QuerySerializerOptions { get; }
        public JsonSerializerOptions DataSerializerOptions { get; }
        public string NativeQueryEndpointUri { get; private set; } = "druid/v2";

        public DataSourceProviderBuilder ConfigureClient(Action<IHttpClientBuilder> configure)
        {
            configure(ClientBuilder);
            return this;
        }

        public DataSourceProviderBuilder ConfigureQuerySerializer(Action<JsonSerializerOptions> configure)
        {
            configure(QuerySerializerOptions);
            return this;
        }

        public DataSourceProviderBuilder ConfigureDataSerializer(Action<JsonSerializerOptions> configure)
        {
            configure(DataSerializerOptions);
            return this;
        }

        public DataSourceProviderBuilder SetNativeQueryEndpointUri(string toValue)
        {
            NativeQueryEndpointUri = toValue;
            return this;
        }
    }

    public static class ServiceCollectionExtensions
    {
        public static DataSourceProviderBuilder AddDataSourceProvider<TProvider>(
            this IServiceCollection services, Uri druidRouterUri)
            where TProvider : DataSourceProvider
        {
            var clientId = Guid.NewGuid().ToString();
            var clientBuilder = services.AddHttpClient(clientId);
            clientBuilder.ConfigureHttpClient(client => client.BaseAddress = druidRouterUri);
            var querySerlializerOptions = DefaultSerializerOptions.Query.Create();
            var dataSerializerOptions = DefaultSerializerOptions.Data.Create();
            var providerBuilder = new DataSourceProviderBuilder(clientBuilder, querySerlializerOptions, dataSerializerOptions);
            services
                .AddSingleton<IDataSourceInitializer, TProvider>()
                .AddSingleton(services =>
                {
                    var all = services.GetServices<IDataSourceInitializer>();
                    var match = all
                        .Where(initializer => initializer.GetType() == typeof(TProvider))
                        .ToArray();
                    var matchSingle = match.Length == 1 ?
                        match[0] :
                        throw new InvalidOperationException($"{typeof(TProvider)} has been registered multiple times.");
                    var factory = services.GetRequiredService<IHttpClientFactory>();
                    var options = new DataSourceOptions(
                        querySerlializerOptions,
                        dataSerializerOptions,
                        () => factory.CreateClient(clientId),
                        () => providerBuilder.NativeQueryEndpointUri);
                    matchSingle.Initialize(options);
                    return (TProvider)matchSingle;
                });
            return providerBuilder;
        }
    }
}
