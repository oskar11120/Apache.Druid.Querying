using FluentAssertions;
using Toxiproxy.Net.Toxics;
using static Apache.Druid.Querying.Tests.Integration.ServiceProvider;

namespace Apache.Druid.Querying.Tests.Integration;

internal class Given2MBQuery_TruncatedResultHandlingShouldHandle : TruncatedResultHandlingShouldHandle<ScanResult<Edit>>
{
    protected override IQueryWith.SourceAndResult<Edit, ScanResult<Edit>> Query
        => new Query<Edit>
        .Scan()
        .Interval(new(DateTimeOffset.MinValue, DateTimeOffset.MaxValue))
        .Context(new QueryContext.Scan { PopulateCache = false, PopulateResultLevelCache = false, UseCache = false, UseResultLevelCache = false, MaxQueuedBytes = 10000000 });
}

internal sealed record Aggregations(
    DateTimeOffset? Timestamp,
    bool? IsRobot,
    string? Channel,
    string? Flags,
    bool? IsUnpatrolled,
    string? Page,
    string? DiffUri,
    int? Added,
    string? Comment,
    int? CommentLength,
    bool? IsNew,
    bool? IsMinor,
    int? Delta,
    bool? IsAnonymous,
    string? User,
    int? DeltaBucket,
    int? Deleted,
    string? Namespace,
    string? CityName,
    string? CountryName,
    string? RegionIsoCode,
    int? MetroCode,
    string? CountryIsoCode,
    string? RegionName,
    int? Count);
internal sealed record PostAggregations(
    double? One,
    double? Two,
    double? Three,
    double? Four,
    double? Five,
    double? Six,
    double? Seven,
    double? Eight,
    double? Nine,
    double? Ten,
    double? Eleven,
    double? Twelve,
    double? Thirteen,
    double? Fourteen,
    double? Fifteen,
    double? Sixteen,
    double? SevenTeen,
    double? Eighteen,
    double? Nineteen,
    double? Twenty,
    double? TwentyOne,
    double? TwentyTwo,
    double? TwentyThree,
    double? TwentyFour);

internal class Given5MBQuery_TruncatedResultHandlingShouldHandle
    : TruncatedResultHandlingShouldHandle<WithTimestamp<Aggregations_PostAggregations<Aggregations, PostAggregations>>>
{
    protected override IQueryWith.SourceAndResult<Edit, WithTimestamp<Aggregations_PostAggregations<Aggregations, PostAggregations>>> Query
        => new Query<Edit>
        .TimeSeries
        .WithNoVirtualColumns
        .WithAggregations<Aggregations>
        .WithPostAggregations<PostAggregations>()
        .Interval(new(DateTimeOffset.MinValue, DateTimeOffset.MaxValue))
        .Aggregations(type => new Aggregations(
            type.First(edit => edit.Timestamp),
            type.First(edit => edit.IsRobot),
            type.First(edit => edit.Channel),
            type.First(edit => edit.Flags),
            type.First(edit => edit.IsUnpatrolled),
            type.First(edit => edit.Page!),
            type.First(edit => edit.DiffUri),
            type.First(edit => edit.Added),
            type.First(edit => edit.Comment),
            type.First(edit => edit.CommentLength),
            type.First(edit => edit.IsNew),
            type.First(edit => edit.IsMinor),
            type.First(edit => edit.Delta!),
            type.First(edit => edit.IsAnonymous),
            type.First(edit => edit.User),
            type.First(edit => edit.DeltaBucket),
            type.First(edit => edit.Deleted),
            type.First(edit => edit.Namespace),
            type.First(edit => edit.CityName),
            type.First(edit => edit.CountryName),
            type.First(edit => edit.RegionIsoCode),
            type.First(edit => edit.MetroCode),
            type.First(edit => edit.CountryIsoCode),
            type.First(edit => edit.RegionName),
            type.Count()))
        .PostAggregations(type => new PostAggregations(
            type.Expression<double>(data => $"{data.Added}*0.2333321321321", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.3333321321321", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.4333321321321", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.5333321321321", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.6333321321321", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.7333321321321", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.8333321321321", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.9333321321321", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.11333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.12333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.13333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.14333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.15333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.16333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.17333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.18333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.19333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.21333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.21333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.23333321312312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.2333321321312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.3333321321312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.4333321321312", SimpleDataType.Double),
            type.Expression<double>(data => $"{data.Added}*0.5333321321312", SimpleDataType.Double)))
        .Granularity(SimpleGranularity.Second)
        .Context(new QueryContext.TimeSeries { PopulateCache = false, PopulateResultLevelCache = false, UseCache = false, UseResultLevelCache = false });


    protected override bool IsEmpty(WithTimestamp<Aggregations_PostAggregations<Aggregations, PostAggregations>> result)
        => result.Value.Aggregations.Timestamp is null;
}

[NonParallelizable, Order(1)]
internal abstract class TruncatedResultHandlingShouldHandle<TResult>
{
    protected abstract IQueryWith.SourceAndResult<Edit, TResult> Query { get; }
    protected virtual bool IsEmpty(TResult result) => false;

    [SetUp]
    public Task SetUp() => ToxiProxy.Reset();

    private async Task GivenNetworkError_Query_WithNoHandling_ShouldThrow()
    {
        await Wikipedia_UnderToxiproxy
            .Edits
            .Awaiting(edits => edits.ExecuteQuery(Query, onTruncatedResultsQueryRemaining: false).ToArrayAsync())
            .Should()
            .ThrowAsync<IOException>()
            .WithMessage("The response ended prematurely*");
    }

    private async Task ResultsWithHandling_AndNetworkErrors_ShouldBeTheSameAs_ResultsWithNoHandling_AndNoNetworkErrors()
    {
        IAsyncEnumerable<TResult> ExecuteQueryGetNonEmptyResults(WikipediaDataSourceProvider wikipedia)
            => wikipedia
            .Edits
            .ExecuteQuery(Query)
            .Where(result => !IsEmpty(result));
        var withHandling = ExecuteQueryGetNonEmptyResults(Wikipedia_UnderToxiproxy);
        var withNoHandling = ExecuteQueryGetNonEmptyResults(Wikipedia);
        var zipped = withHandling.Zip(withNoHandling);
        await foreach (var (one, other) in zipped)
            one.Should().BeEquivalentTo(other);
    }

    [Test]
    public async Task TcpFin()
    {
        var dataLimit = new LimitDataToxic
        {
            Stream = ToxicDirection.DownStream,
            Attributes = new() { Bytes = 1024 * 1024 / 2 } // 0.5 MB
        };
        await ToxiProxy.AddAsync(dataLimit);
        await GivenNetworkError_Query_WithNoHandling_ShouldThrow();
        await ResultsWithHandling_AndNetworkErrors_ShouldBeTheSameAs_ResultsWithNoHandling_AndNoNetworkErrors();
    }

    [Test]
    public async Task TcpRst()
    {
        var rateLimit = new BandwidthToxic
        {
            Stream = ToxicDirection.DownStream,
            Attributes = new() { Rate = 1024 / 2 } // 0.5 MB/s
        };
        var reset = new ResetPeerToxic
        {
            Stream = ToxicDirection.DownStream,
            Attributes = new() { Timeout = 0 }
        };
        await ToxiProxy.AddAsync(rateLimit);

        // Workaround to Toxiproxy not having a way to do a tcp reset in the middle of sending http response.
        async Task KeepResettingForHalfASecond()
        {
            await Task.Delay(TimeSpan.FromSeconds(1));
            reset = await ToxiProxy.AddAsync(reset);
            await Task.Delay(TimeSpan.FromSeconds(0.5));
            await ToxiProxy.RemoveToxicAsync(reset.Name);
        }
        var resetTask = KeepResettingForHalfASecond();
        await GivenNetworkError_Query_WithNoHandling_ShouldThrow();
        await resetTask;
        _ = KeepResettingForHalfASecond();
        await ResultsWithHandling_AndNetworkErrors_ShouldBeTheSameAs_ResultsWithNoHandling_AndNoNetworkErrors();
    }

    private sealed class ResetPeerToxic : ToxicBase
    {
        public sealed class ToxicAttributes
        {
            public int Timeout { get; set; }
        }

        public ToxicAttributes? Attributes { get; set; }

        public override string Type => "reset_peer";
    }
}
