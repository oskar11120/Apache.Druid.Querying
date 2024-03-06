
using Apache.Druid.Querying;
using Apache.Druid.Querying.Tests.Integration;
using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Model.Compose;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Nodes;

internal static class DruidSetup
{
    public static readonly Uri RouterUri = new("http://localhost:8888/");

    public static IDisposable CreateAndStartContainers()
    {
        var service = new Builder()
             .UseContainer()
             .UseCompose()
             .FromFile(Path.Combine(Directory.GetCurrentDirectory(), "Setup", "druid-docker-compose.yml"))
             .AssumeComposeVersion(ComposeVersion.V2)
             .WaitForHttp(
                 "router",
                 RouterUri.AbsoluteUri + "status/health",
                 continuation: (response, _) => response.Code < HttpStatusCode.InternalServerError ? 0 : (long)TimeSpan.FromSeconds(0.5).TotalMilliseconds)
             .Build();
        return service.Start();
    }

    public static async Task IngestWikipediaEdits(IHttpClientFactory clientFactory, WikipediaDataSourceProvider wikipedia)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var ingestedDataSources = await wikipedia
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
        var tasksUri = RouterUri.AbsoluteUri + "druid/indexer/v1/task";
        using var client = clientFactory.CreateClient();
        using var response = await client.PostAsJsonAsync(tasksUri, JsonObject.Parse(spec), timeout.Token);
        var test = await response.Content.ReadAsStringAsync();
        response.EnsureSuccessStatusCode();
        var task = (await response.Content.ReadFromJsonAsync<JsonObject>())!["task"]!.GetValue<string>();
        TestContext.WriteLine($"Ingesting wikipedia edits ({task}).");
        const string running = "RUNNING";
        const string success = "SUCCESS";
        var status = running;
        while (status == running)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), timeout.Token);
            var getStatusResult = await client.GetFromJsonAsync<JsonObject>(tasksUri + $"/{task}/{nameof(status)}", timeout.Token);
            status = getStatusResult![nameof(status)]![nameof(status)]!.GetValue<string>();
            TestContext.WriteLine(status);
        }

        if (status != success)
            throw new HttpRequestException("Failed to ingest wikipedia edits.");

        // Workaround to Druid returning incomplete data right after ingestion.
        await Task.Delay(TimeSpan.FromSeconds(5));
    }
}
