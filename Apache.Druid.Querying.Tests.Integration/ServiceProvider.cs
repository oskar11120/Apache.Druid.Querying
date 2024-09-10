global using ToxiProxy_ = Toxiproxy.Net.Proxy;
using Apache.Druid.Querying.Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using System.Net.Http.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Tests.Integration;

[SetUpFixture]
internal class ServiceProvider
{
    public static WikipediaDataSourceProvider Wikipedia => Services.GetRequiredService<WikipediaDataSourceProvider>();
    public static WikipediaDataSourceProvider Wikipedia_UnderToxiproxy => Services.GetRequiredService<WikipediaDataSourceProvider_UnderToxiproxy>();
    public static ToxiProxy_ ToxiProxy { get; private set; }
    public static IServiceProvider Services { get; private set; }

    private static IDisposable? containers;
    private static IAsyncDisposable? services;

    [OneTimeSetUp]
    protected static async Task SetUp()
    {
        static void AddRetryPolicy(IHttpClientBuilder clientBuilder) => clientBuilder
            .AddTransientHttpErrorPolicy(policy => policy.WaitAndRetryAsync(
                25,
                _ => TimeSpan.FromSeconds(1),
                async (result, _) =>
                {
                    var message = result.Exception?.ToString() ??
                        result.Result.ToString() + Environment.NewLine + await result.Result.Content.ReadAsStringAsync();
                    TestContext.Out.WriteLine(message);
                }));
        var collection = new ServiceCollection();
        collection
            .AddHttpClient()
            .AddDataSourceProvider<WikipediaDataSourceProvider>(TestEnvironment.DruidRouterUri)
            .ConfigureClient(AddRetryPolicy);
        collection
            .AddHttpClient()
            .AddSingleton<Toxiproxy.Net.IHttpClientFactory, ToxiproxyClientFactory>()
            .AddSingleton<Toxiproxy.Net.Client>()
            .AddDataSourceProvider<WikipediaDataSourceProvider_UnderToxiproxy>(TestEnvironment.DruidRouterUri_UnderToxiproxy)
            .ConfigureClient(AddRetryPolicy);
        var provider = collection.BuildServiceProvider();
        services = provider;
        Services = provider;
        containers = TestEnvironment.CreateAndStart();

        var proxyTask = SetUpToxiproxyForWikipedia();
        await Task.WhenAll(proxyTask, IngestWikipediaEdits());
        ToxiProxy = await proxyTask;
    }

    [OneTimeTearDown]
    protected static async Task TearDown()
    {
        // Dispose if to be recreated on each test run.
        containers = null;

        containers?.Dispose();
        await (services?.DisposeAsync() ?? ValueTask.CompletedTask);
    }

    private static async Task IngestWikipediaEdits()
    {
        var clientFactory = Services.GetRequiredService<IHttpClientFactory>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var ingestedDataSources = await Wikipedia
            .Edits
            .ExecuteQuery(new Query<Edit>.DataSourceMetadata(), token: timeout.Token)
            .ToArrayAsync(timeout.Token);
        if (ingestedDataSources.Any())
        {
            return;
        }

        var spec = """
        {
          "type": "index_parallel",
          "spec": {
            "ioConfig": {
              "type": "index_parallel",
              "inputSource": {
                "type": "http",
                "uris": [
                  "https://druid.apache.org/data/wikipedia.json.gz"
                ]
              },
              "inputFormat": {
                "type": "json"
              }
            },
            "dataSchema": {
              "granularitySpec": {
                "segmentGranularity": "day",
                "queryGranularity": "none",
                "rollup": false
              },
              "dataSource": "wikipedia",
              "timestampSpec": {
                "column": "timestamp",
                "format": "iso"
              },
              "dimensionsSpec": {
                "dimensions": [
                  "isRobot",
                  "channel",
                  "flags",
                  "isUnpatrolled",
                  "page",
                  "diffUrl",
                  {
                    "type": "long",
                    "name": "added"
                  },
                  "comment",
                  {
                    "type": "long",
                    "name": "commentLength"
                  },
                  "isNew",
                  "isMinor",
                  {
                    "type": "long",
                    "name": "delta"
                  },
                  "isAnonymous",
                  "user",
                  {
                    "type": "long",
                    "name": "deltaBucket"
                  },
                  {
                    "type": "long",
                    "name": "deleted"
                  },
                  "namespace",
                  "cityName",
                  "countryName",
                  "regionIsoCode",
                  "metroCode",
                  "countryIsoCode",
                  "regionName"
                ]
              }
            },
            "tuningConfig": {
              "type": "index_parallel",
              "partitionsSpec": {
                "type": "dynamic"
              }
            }
          }
        }
        """;
        var tasksUri = TestEnvironment.DruidRouterUri + "druid/indexer/v1/task";
        using var client = clientFactory.CreateClient();
        using var response = await client.PostAsJsonAsync(tasksUri, JsonNode.Parse(spec), timeout.Token);
        var test = await response.Content.ReadAsStringAsync();
        response.EnsureSuccessStatusCode();
        var task = (await response.Content.ReadFromJsonAsync<JsonObject>())!["task"]!.GetValue<string>();
        TestContext.Out.WriteLine($"Ingesting wikipedia edits ({task}).");
        const string running = "RUNNING";
        const string success = "SUCCESS";
        var status = running;
        while (status == running)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), timeout.Token);
            var getStatusResult = await client.GetFromJsonAsync<JsonObject>(tasksUri + $"/{task}/{nameof(status)}", timeout.Token);
            status = getStatusResult![nameof(status)]![nameof(status)]!.GetValue<string>();
            TestContext.Out.WriteLine(status);
        }

        if (status != success)
            throw new HttpRequestException("Failed to ingest wikipedia edits.");

        // Workaround to Druid returning incomplete data right after ingestion.
        await Task.Delay(TimeSpan.FromSeconds(5));
    }

    private static async Task<ToxiProxy_> SetUpToxiproxyForWikipedia()
    {
        const string id = "DruidRouter";
        var client = Services.GetRequiredService<Toxiproxy.Net.Client>();
        var all = await client.AllAsync();
        var proxy = all.Values.SingleOrDefault(proxy => proxy.Name == id);
        if (proxy is null)
        {
            proxy = new ToxiProxy_
            {
                Enabled = true,
                Listen = $"toxiproxy:{TestEnvironment.DruidRouterUri_UnderToxiproxy.Port}",
                Upstream = $"router:{TestEnvironment.DruidRouterUri.Port}",
                Name = id
            };
            await client.AddAsync(proxy);
        }
        else
            await proxy.Reset();
        return proxy;
    }

    private sealed class WikipediaDataSourceProvider_UnderToxiproxy : WikipediaDataSourceProvider
    {
    }

    private sealed class ToxiproxyClientFactory(IHttpClientFactory @base) : Toxiproxy.Net.IHttpClientFactory
    {
        public Uri BaseUrl { get; } = TestEnvironment.ToxiproxyUri;
        private readonly IHttpClientFactory @base = @base;

        public HttpClient Create()
        {
            var client = @base.CreateClient();
            client.BaseAddress = BaseUrl;
            client.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json");
            return client;
        }
    }
}

public static class ToxiproxyExtensions
{
    public static async Task<ToxiProxy_> Reset(this ToxiProxy_ proxy)
    {
        var client = ServiceProvider.Services.GetRequiredService<Toxiproxy.Net.Client>();
        await client.DeleteAsync(proxy);
        return await client.AddAsync(proxy);
    }
}
