﻿using FluentAssertions;
using Snapshooter;
using Snapshooter.NUnit;
using System.Globalization;
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
}

internal class QueryShould_ReturnRightData
{
    private static async Task VerifyMatch<TSource, TResult, TContext>(
        DataSource<TSource> dataSource,
        IQueryWithSource<TSource>.AndResult<TResult>.AndDeserializationAndTruncatedResultHandling<TContext> query,
        [CallerArgumentExpression(nameof(query))] string snapshotNameSuffix = "")
        where TContext : new()
    {
        var spanshotName = Snapshot.FullName(new SnapshotNameExtension(snapshotNameSuffix));
        var json = dataSource.MapQueryToJson(query).ToString();
        await TestContext.Out.WriteLineAsync(json);
        void Match(List<TResult> results)
            => Snapshot.Match(results, spanshotName, options => options.IgnoreField("[:].SegmentId"));
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
        IQueryWithSource<Edit>.AndResult<TResult>.AndDeserializationAndTruncatedResultHandling<TContext> query,
        [CallerArgumentExpression(nameof(query))] string snapshotNameSuffix = "")
        where TContext : new()
        => VerifyMatch(Wikipedia.Edits, query, snapshotNameSuffix);

    [Test]
    public async Task Wrapped()
    {
        var first = new Query<Edit>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<TimeSeriesAggregations>()
            .Aggregations(type => new(
                type.Count(),
                type.Sum(edit => edit.Added)))
            .DefaultInterval()
            .Granularity(Granularity.FifteenMinutes)
            .Context(new() { SkipEmptyBuckets = true });
        var wrapped = Wikipedia
            .Edits
            .WrapOverQuery(first);
        var query = new Query<WithTimestamp<TimeSeriesAggregations>>
            .Scan()
            .Interval(TestData.Interval)
            .Filter(type => type.Not(type.Selector(data => data.Value.Count, 6)))
            .Limit(1000);
        await VerifyMatch(wrapped, query, string.Empty);
    }

    private record Country(string Code, string FullName);
    [Test]
    [Ignore(
        "Joining inline data sources does not seem to work, even though druid docs suggest it should: " +
        "(https://druid.apache.org/docs/latest/querying/datasource#join).")]
    public async Task Join()
    {
        var inline = Wikipedia
            .Inline(new Country[]
            {
                new("US", "United States")
            });
        var join = Wikipedia
            .Edits
            .LeftJoin(inline, data => $"{data.Left.CountryIsoCode} == {data.Right.Code}");
        var query = new Query<LeftJoinResult<Edit, Country>>
            .Scan()
            .Interval(TestData.Interval)
            .Limit(100);
        await VerifyMatch(join, query);
    }

    [Test]
    public async Task Union()
    {
        var union = Wikipedia
            .Edits
            .Union(Wikipedia
                .Edits);
        var query = new Query<Union<Edit, Edit>>
            .Scan()
            .Interval(TestData.Interval)
            .Limit(10);
        await VerifyMatch(union, query, string.Empty);
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
                new[] { "zero" }));
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

    [Test]
    public async Task Scan()
    {
        var first3Us = new Query<Edit>
            .Scan()
            .DefaultInterval()
            .Filter(type => type.Selector(
                edit => edit.CountryIsoCode,
                "US"))
            .Limit(3);
        await VerifyMatch(first3Us);
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
            .Granularity(Granularity.EightHours);

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
            .Granularity(Granularity.Hour);
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
            .Order(OrderDirection.Descending)
            .Aggregations(type => new(
                type.Count(),
                type.Sum(edit => edit.Added)))
            .PostAggregations(type => new(type.Arithmetic(
                ArithmeticFunction.Divide,
                type.FieldAccess(aggregations => aggregations.TotalAdded),
                type.FieldAccess(aggregations => aggregations.Count))))
            .Filter(type => type.Selector(edit => edit.CountryIsoCode, "US"))
            .DefaultInterval()
            .Granularity(Granularity.Hour)
            .Context(new QueryContext.TimeSeries() { SkipEmptyBuckets = true });
        await VerifyMatch(lineStatisticsPerHour);

        var inEuWarsawTime = lineStatisticsPerHour
            .Granularity(Granularity.Hour, "Europe/Warsaw");
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
                .Granularity(Granularity.Hour)
                .Dimensions(type => new(
                    type.Default(data => data.Robot),
                    type.Default(data => data.IsNew)))
                .Aggregations(type => type.Count());
            if(i is 1)
                await VerifyMatch(dataSource, two);
        }

        await Test(Wikipedia.Edits);
    }

    interface IIotMeasurement
    {
        Guid ObjectId { get; }
        double Value { get; }
    }
}
