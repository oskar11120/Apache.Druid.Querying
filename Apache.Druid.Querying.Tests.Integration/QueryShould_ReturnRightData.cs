using Apache.Druid.Querying.Internal;
using FluentAssertions;
using LinqKit;
using Snapshooter;
using Snapshooter.NUnit;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using static Apache.Druid.Querying.Tests.Integration.ServiceProvider;

namespace Apache.Druid.Querying.Tests.Integration;

internal static class TestData
{
    public static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2016-06-27T00:00:00.000Z", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
    public static readonly Interval Interval = new(T0, T0.AddDays(1));

    public static TQuery DefaultInterval<TQuery>(this TQuery query)
        where TQuery : IQueryWith.Intervals
        => query
            .Interval(Interval);

    public static Query<TSource>.Scan.WithColumns<SomeEditColumns> SomeColumnsOnly<TSource>(
        this Query<TSource>.Scan.WithColumns<SomeEditColumns> query,
        Expression<Func<TSource, Edit>> getEdit)
        => query.Columns(
            ((Expression<Func<TSource, SomeEditColumns>>)
            (source => SomeEditColumns.FromEditExpression.Invoke(getEdit.Invoke(source))))
            .Expand());
}

internal record SomeEditColumns(
    DateTimeOffset Time,
    bool Robot,
    string Channel,
    string Flags,
    bool Unpatrolled,
    string PageId,
    string Diff)
{
    public static readonly Expression<Func<Edit, SomeEditColumns>> FromEditExpression = edit => new(
        edit.Timestamp,
        edit.IsRobot,
        edit.Channel,
        edit.Flags,
        edit.IsUnpatrolled,
        edit.Page,
        edit.DiffUri);

    public static readonly Func<Edit, SomeEditColumns> FromEdit = FromEditExpression.Compile();
}

[Order(0)]
internal class QueryShould_ReturnRightData
{
    private static async Task VerifyMatch<TSource, TResult, TContext>(
        DataSource<TSource> dataSource,
        TruncatedQueryResultHandler<TSource>.Base<TResult, TContext> query,
        [CallerArgumentExpression(nameof(query))] string snapshotNameSuffix = "")
        where TContext : new()
    {
        var spanshotName = Snapshot.FullName(new SnapshotNameExtension(snapshotNameSuffix));
        var json = dataSource.MapQueryToJson(query);
        json["isSourceEditVerifiedOnMapToJson"]?.GetValue<bool>().Should().BeTrue();
        await TestContext.Out.WriteLineAsync(json.ToString());
        void Match(List<TResult> results)
            => Snapshot.Match(results, spanshotName, options => options.IgnoreField("[:].SegmentId").IgnoreField("[:].Id"));
        var results = await dataSource
            .ExecuteQuery(query)
            .ToListAsync();
        Match(results);

        var withNoTruncatedResultHandling = await dataSource
            .ExecuteQuery(query, onTruncatedResultsQueryRemaining: false)
            .ToListAsync();
        Match(withNoTruncatedResultHandling);
    }

    private static Task VerifyMatch<TResult, TContext>(
        TruncatedQueryResultHandler<Edit>.Base<TResult, TContext> query,
        [CallerArgumentExpression(nameof(query))] string snapshotNameSuffix = "")
        where TContext : new()
        => VerifyMatch(Wikipedia.Edits, query, snapshotNameSuffix);

    [Test]
    public async Task FilteredAggregation()
    {
        static Query<Edit>.TimeSeries.WithNoVirtualColumns.WithAggregations<int> Query() => new Query<Edit>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<int>()
            .DefaultInterval()
            .Granularity(SimpleGranularity.Hour);
        var aggregationFilteredQuery = Query()
            .Aggregations(type => type.Filtered(
                filter => filter.And(
                    filter.Selector(data => data.IsNew, true),
                    filter.Selector(data => data.IsRobot, true)),
                type.Count()));
        await VerifyMatch(aggregationFilteredQuery, string.Empty);

        var normallyFilteredQuery = Query()
            .Filter(type => type.And(
                type.Selector(data => data.IsNew, true),
                type.Selector(data => data.IsRobot, true)))
            .Aggregations(type => type.Count());
        var aggregation = await Wikipedia.Edits.ExecuteQuery(aggregationFilteredQuery).ToListAsync();
        var normal = await Wikipedia.Edits.ExecuteQuery(normallyFilteredQuery).ToListAsync();
        aggregation.Should().BeEquivalentTo(normal);
    }

    [Test]
    public async Task Nested()
    {
        var first = new Query<Edit>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<TimeSeriesAggregations>()
            .Aggregations(type => new(
                type.Count(),
                type.Sum(edit => edit.Added)))
            .DefaultInterval()
            .Granularity(SimpleGranularity.FifteenMinutes)
            .Context(new() { SkipEmptyBuckets = true });
        var wrapped = Wikipedia
            .Edits
            .ToQueryDataSource(first);
        var query = new Query<WithTimestamp<TimeSeriesAggregations>>
            .Scan()
            .Interval(TestData.Interval)
            .Filter(type => type.Not(type.Selector(data => data.Value.Count, 6)))
            .Limit(1000);
        await VerifyMatch(wrapped, query, string.Empty);
    }

    public enum JoinTestCase
    {
        LatestEditTimestampsPerCountry,
        LatestEditsPerCountry,
        LatestEditsPerCountry_SomeColumnsOnly,
        ConsistencyBetweenAll
    }
    private record Country(string Code, string FullName);
    [TestCase(JoinTestCase.LatestEditTimestampsPerCountry)]
    [TestCase(JoinTestCase.LatestEditsPerCountry)]
    [TestCase(JoinTestCase.LatestEditsPerCountry_SomeColumnsOnly)]
    [TestCase(JoinTestCase.ConsistencyBetweenAll)]
    public async Task Join(JoinTestCase @case)
    {
        var latestEditTimestampsPerCountry_query = new Query<Edit>
            .GroupBy<string>
            .WithNoVirtualColumns
            .WithAggregations<DateTimeOffset>()
            .DefaultInterval()
            .Granularity(SimpleGranularity.Day)
            .Dimensions(type => type.Default(data => data.CountryName))
            .Aggregations(type => type.Last(data => data.Timestamp));
        var latestEditTimestampsPerCountry_dataSource = Wikipedia.Edits.ToQueryDataSource(latestEditTimestampsPerCountry_query);
        var lastestEditsPerCountry_dataSource = Wikipedia.Edits.InnerJoin(
            latestEditTimestampsPerCountry_dataSource,
            data => $"{data.Left.CountryName} == {data.Right.Value.Dimensions} && {data.Left.Timestamp} == {data.Right.Value.Aggregations}");
        var edits_query = new Query<InnerJoinData<Edit, WithTimestamp<Dimensions_Aggregations<string, DateTimeOffset>>>>
            .Scan()
            .DefaultInterval();
        var edits_someColumnsOnly_query = new Query<InnerJoinData<Edit, WithTimestamp<Dimensions_Aggregations<string, DateTimeOffset>>>>
            .Scan
            .WithColumns<SomeEditColumns>()
            .SomeColumnsOnly(data => data.Left)
            .DefaultInterval();
        await (@case switch
        {
            JoinTestCase.LatestEditTimestampsPerCountry => VerifyMatch(Wikipedia.Edits, latestEditTimestampsPerCountry_query, string.Empty),
            JoinTestCase.LatestEditsPerCountry => VerifyMatch(lastestEditsPerCountry_dataSource, edits_query, string.Empty),
            JoinTestCase.LatestEditsPerCountry_SomeColumnsOnly => VerifyMatch(lastestEditsPerCountry_dataSource, edits_someColumnsOnly_query, string.Empty),
            JoinTestCase.ConsistencyBetweenAll => VerifyConsistency(),
            _ => throw new NotSupportedException()
        });

        async Task VerifyConsistency()
        {
            var edits = await lastestEditsPerCountry_dataSource
                .ExecuteQuery(edits_query)
                .Select(result => result.Value)
                .OrderBy(data => data.Right.Value.Aggregations)
                .ToArrayAsync();

            var timestamps_fromEdits = edits
                .Select(data => data.Right.Value);
            var timestamps = await Wikipedia.Edits
                .ExecuteQuery(latestEditTimestampsPerCountry_query)
                .Select(result => result.Value)
                .Where(result => result.Dimensions is not null)
                .OrderBy(result => result.Aggregations)
                .ToArrayAsync();
            timestamps_fromEdits.Should().BeEquivalentTo(timestamps);

            var someColumns_fromEdits = edits
                .Select(data => SomeEditColumns.FromEdit(data.Left));
            var someColumns = await lastestEditsPerCountry_dataSource
                .ExecuteQuery(edits_someColumnsOnly_query)
                .Select(result => result.Value)
                .ToListAsync();
            someColumns_fromEdits.Should().BeEquivalentTo(someColumns);
        }
    }

    [Test]
    public async Task Union()
    {
        var union = Wikipedia
            .Edits
            .Union<Edit>(Wikipedia.Edits);
        var unionQuery = new Query<Union<Edit, Edit>>
            .Scan()
            .Interval(TestData.Interval)
            .Limit(10);
        await VerifyMatch(union, unionQuery, string.Empty);

        var unionResult = await union.ExecuteQuery(unionQuery).Select(result => result.Value.First).ToArrayAsync();
        var sameSourceUnion = Wikipedia
            .Edits
            .Union(Wikipedia.Edits);
        var sameSourceUnionQuery = new Query<Edit>
            .Scan()
            .Interval(TestData.Interval)
            .Limit(10);
        var sameSourceUnionResult = await sameSourceUnion.ExecuteQuery(sameSourceUnionQuery).Select(result => result.Value).ToArrayAsync();
        unionResult.Should().BeEquivalentTo(sameSourceUnionResult, options => options.WithStrictOrdering());
    }

    private sealed record InlineData(string Word, [property: DataSourceColumn("Integer")] int Number);
    [Test]
    public async Task Inline()
    {
        var query = new Query<InlineData>
            .Scan()
            .Interval(new(default, DateTimeOffset.MaxValue))
            .Filter(type => type.In(
                data => data.Word,
                ["zero"]));
        var inline = Wikipedia
            .Inline(new InlineData[]
            {
                new("zero", 1),
                new("one", 2)
            });
        var result = await inline
            .ExecuteQuery(query)
            .ToListAsync();
        result
            .Single()
            .Value
            .Should()
            .Be(new InlineData("zero", 1));
    }

    public enum ScanTestCase
    {
        First3Us_AllColumns,
        First3Us_SomeColumnsOnly,
        First3Us_ConsistencyIndependentOfSelectedColumns,
        First3Us_Descending
    }

    [TestCase(ScanTestCase.First3Us_AllColumns)]
    [TestCase(ScanTestCase.First3Us_SomeColumnsOnly)]
    [TestCase(ScanTestCase.First3Us_ConsistencyIndependentOfSelectedColumns)]
    [TestCase(ScanTestCase.First3Us_Descending)]
    public async Task Scan(ScanTestCase @case)
    {
        var first3Us = new Query<Edit>
            .Scan()
            .DefaultInterval()
            .Filter(type => type.Selector(
                edit => edit.CountryIsoCode,
                "US"))
            .Limit(3);
        var descending = first3Us.Copy().Order(OrderDirection.Descending);
        var someColumnsOnly = new Query<Edit>
            .Scan
            .WithColumns<SomeEditColumns>()
            .DefaultInterval()
            .Filter(type => type.Selector(
                edit => edit.CountryIsoCode,
                "US"))
            .Limit(3)
            .SomeColumnsOnly(edit => edit);
        async Task VerifyColumnsConsistency()
        {
            var first = await Wikipedia
                .Edits
                .ExecuteQuery(first3Us)
                .Select(result => result.Value)
                .Select(SomeEditColumns.FromEdit)
                .ToListAsync();
            var second = await Wikipedia
                .Edits
                .ExecuteQuery(someColumnsOnly)
                .Select(result => result.Value)
                .ToListAsync();
            first.Should().BeEquivalentTo(second);
        }
        var task = @case switch
        {
            ScanTestCase.First3Us_AllColumns => VerifyMatch(first3Us, string.Empty),
            ScanTestCase.First3Us_SomeColumnsOnly => VerifyMatch(someColumnsOnly, string.Empty),
            ScanTestCase.First3Us_ConsistencyIndependentOfSelectedColumns => VerifyColumnsConsistency(),
            ScanTestCase.First3Us_Descending => VerifyMatch(descending, string.Empty),
            _ => throw new NotSupportedException()
        };
        await task;
    }

    [Test]
    public async Task GroupBy()
    {
        var countsPerCountry = new Query<Edit>
            .GroupBy<string>
            .WithNoVirtualColumns
            .WithAggregations<int>()
            .DefaultInterval()
            .Dimensions(type => type.Default(edit => edit.CountryName))
            .Aggregations(type => type.Count())
            .Granularity(SimpleGranularity.EightHours);

        var countsPerCountryOrderedByCount = countsPerCountry
            .LimitSpec(limit: 100, offset: 1, columns => columns.OrderBy(data => data.Aggregations, OrderDirection.Descending, SortingOrder.Numeric));
        await VerifyMatch(countsPerCountryOrderedByCount);

        var countsPerValidCountry = countsPerCountry
            .Filter(type => type.Not(type.Selector(edit => edit.CountryName, string.Empty)));
        await VerifyMatch(countsPerValidCountry);
    }

    [Test]
    public async Task TopN()
    {
        var countsPerPage = new Query<Edit>
            .TopN<string>
            .WithNoVirtualColumns
            .WithAggregations<int>()
            .Dimension(type => type.Default(edit => edit.Page))
            .Threshold(10)
            .Aggregations(type => type.Count())
            .Metric(type => type.Numeric(data => data.Aggregations))
            .DefaultInterval()
            .Granularity(SimpleGranularity.Hour);
        await VerifyMatch(countsPerPage);

        var usAnonymousCountsPerPage = countsPerPage
            .Filter(type => type.And(
                type.Selector(edit => edit.IsAnonymous, true),
                type.Selector(edit => edit.CountryIsoCode, "US")));
        await VerifyMatch(usAnonymousCountsPerPage);
    }

    private record TimeSeriesAggregations(int Count, int TotalAdded);
    private record TimeSeriesPostAggrgations(double AverageAdded);
    [Test]
    public async Task TimeSeries()
    {
        var lineStatisticsPerHour = new Query<Edit>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<TimeSeriesAggregations>
            .WithPostAggregations<TimeSeriesPostAggrgations>()
            .Descending(true)
            .Aggregations(type => new(
                type.Count(),
                type.Sum(edit => edit.Added)))
            .PostAggregations(type => new(type.Arithmetic(
                ArithmeticFunction.Divide,
                type.FieldAccess(aggregations => aggregations.TotalAdded),
                type.FieldAccess(aggregations => aggregations.Count))))
            .Filter(type => type.Selector(edit => edit.CountryIsoCode, "US"))
            .DefaultInterval()
            .Granularity(SimpleGranularity.Hour)
            .Context(new QueryContext.TimeSeries() { SkipEmptyBuckets = true });
        await VerifyMatch(lineStatisticsPerHour);

        var inEuWarsawTime = lineStatisticsPerHour
            .Granularity(SimpleGranularity.Hour, "Europe/Warsaw");
        await VerifyMatch(inEuWarsawTime);
    }


    private sealed record GroupByBooleans(bool Robot, bool New);
    [TestCase(0)]
    [TestCase(1)]
    public async Task WithDataPropertiesAccessedByInterface(int i)
    {
        async Task Test<TEdit>(DataSource<TEdit> dataSource) where TEdit : IEditBooleans
        {
            var one = new Query<TEdit>
                .Scan()
                .DefaultInterval()
                .Filter(type => type.And(
                    type.Selector(data => data.IsNew, true),
                    type.Selector(data => data.Robot, true)))
                .Limit(100);
            if (i is 0)
                await VerifyMatch(dataSource, one);

            var two = new Query<TEdit>
                .GroupBy<GroupByBooleans>
                .WithNoVirtualColumns
                .WithAggregations<int>()
                .DefaultInterval()
                .Granularity(SimpleGranularity.Hour)
                .Dimensions(type => new(
                    type.Default(data => data.Robot),
                    type.Default(data => data.IsNew)))
                .Aggregations(type => type.Count());
            if (i is 1)
                await VerifyMatch(dataSource, two);
        }

        await Test(Wikipedia.Edits);
    }

    [Test]
    public async Task SegmentMetadata_All()
    {
        var query = new Query<Edit>
            .SegmentMetadata()
            .DefaultInterval()
            .Merge(false)
            .AnalysisTypes(Enum.GetValues<SegmentMetadata.AnalysisType>());
        await VerifyMatch(Wikipedia.Edits, query, string.Empty);
    }

    [Test]
    public async Task SegmentMetadata_Minimal()
    {
        var query = new Query<Edit>.SegmentMetadata();
        await VerifyMatch(Wikipedia.Edits, query, string.Empty);
    }

    interface IIotMeasurement
    {
        Guid ObjectId { get; }
        double Value { get; }
    }
}
