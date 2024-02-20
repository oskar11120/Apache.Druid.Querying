﻿using System;
using System.Net.Http;
using System.Text.Json;

namespace Apache.Druid.Querying.DependencyInjection
{
    public sealed record DataSourceOptions(JsonSerializerOptions Serializer, Func<HttpClient> HttpClientFactory);

    public interface IDataSourceInitializer
    {
        private protected DataSourceOptions? options { get; set; }

        internal DataSourceOptions Options => options ??
            throw new InvalidOperationException($"Attempted to use an uninitialized instance of {GetType()}.");

        bool Initialized => options is not null;

        void Initialize(DataSourceOptions state)
        {
            if (this.options is not null)
                throw new InvalidOperationException("Already initialized.");
            this.options = state;
        }
    }
}
