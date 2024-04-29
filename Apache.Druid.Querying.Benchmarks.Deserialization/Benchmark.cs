using Apache.Druid.Querying.Tests.Integration;
using BenchmarkDotNet.Attributes;
using Apache.Druid.Querying.Json;
using System.Text.Json;
using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Sections;

namespace Apache.Druid.Querying.Benchmarks.Deserialization;

[MemoryDiagnoser]
public class Benchmark
{
    private static readonly JsonSerializerOptions jsonOptions = DefaultSerializerOptions.Create();
    private static readonly Query<Edit>.Scan query = new();
    private static readonly IQueryWith.Result<ScanResult<Edit>> deserializer = query;
    private static readonly SectionAtomicity.ImmutableBuilder atomicity = (query as IQueryWithInternal.SectionAtomicity).SectionAtomicity;
    private static readonly PropertyColumnNameMapping.ImmutableBuilder mappings = PropertyColumnNameMapping.ImmutableBuilder.Create<Edit>();
    private MemoryStream json = null!;
    private byte[] buffer = null!;

    [GlobalSetup]
    public async Task SetUp()
    {
        json = new MemoryStream();
        using var file = File.OpenRead("ScanResult.json");
        await file.CopyToAsync(json);
        buffer = new byte[jsonOptions.DefaultBufferSize];
    }

    [Benchmark]
    public async Task<object?> Logic()
    {
        json.Position = 0;
        var context = new QueryResultDeserializationContext(new(json, buffer, readCount: 0), jsonOptions, atomicity, mappings);
        var results = deserializer.Deserialize(context, CancellationToken.None);
        object? last = null;
        await foreach (var result in results)
            last = result.Value;
        return last;
    }
}
