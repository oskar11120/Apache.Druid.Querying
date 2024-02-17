using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
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

    public static TQuery DefaultInterval<TQuery>(this TQuery query) where TQuery :
        IQueryWith.Intervals
        => query
            .Interval(Interval);

    public static TQuery AggregationsDefaults<TQuery>(this TQuery query) where TQuery :
        IQueryWith.Granularity,
        IQueryWith.Aggregations<VariableMessage, QueryTests_ReturnRightData.Aggregations, TQuery>
        => query
            .Granularity(Granularity.SixHours)
            .Aggregations(factory => new(
                factory.Sum(message => message.Value),
                factory.Count(),
                factory.First(message => message.VariableName),
                factory.First(message => message.Value, SimpleDataType.String)
            ));
}

internal class QueryTests_ReturnRightData
{
    private static WikipediaDataSourceProvider Wikipedia => Services.GetRequiredService<WikipediaDataSourceProvider>();

    private static async Task VerifyMatch<TSource, TResult, TContext>(
        DataSource<TSource> dataSource,
        IQueryWithSource<TSource>.AndResult<TResult>.AndDeserializationAndTruncatedResultHandling<TContext> query,
        [CallerArgumentExpression(nameof(query))] string snapshotNameSuffix = "")
        where TContext : new()
    {
        var json = dataSource.MapQueryToJson(query).ToString();
        await TestContext.Out.WriteLineAsync(json);
        var results = await dataSource
            .ExecuteQuery(query)
            .ToListAsync();
        Snapshot.Match(results, new SnapshotNameExtension(snapshotNameSuffix));
    }

    private static Task VerifyMatch<TResult, TContext>(
        IQueryWithSource<Edit>.AndResult<TResult>.AndDeserializationAndTruncatedResultHandling<TContext> query,
        [CallerArgumentExpression(nameof(query))] string snapshotNameSuffix = "")
        where TContext : new()
        => VerifyMatch(Wikipedia.Edits, query, snapshotNameSuffix);

    [Test]
    public async Task Query_Works()
    {
        var first = new Query<VariableMessage>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>()
            .DefaultInterval()
            .AggregationsDefaults();
        var firstSource = Wikipedia
            .Edits
            .WrapOverQuery(first);
        var firstJson = firstSource.GetJsonRepresentation();
        var second = new Query<WithTimestamp<Aggregations>>
            .Scan()
            .Interval(TestData.Interval)
            .Filter(type => type.Not(type.Selector(first => first.Value.Count, 6)));
        var secondJson = firstSource.MapQueryToJson(second);
        var results = await firstSource
            .ExecuteQuery(second)
            .ToListAsync();
    }

    [Test]
    public async Task Join_Works()
    {
        var inline = Wikipedia
            .Inline(new InlineData[]
            {
                new("pmPAct", "hello!"),
                new("notPmPAct", "goodbye!")
            });
        var join = Wikipedia
            .Edits
            .LeftJoin(inline, data => $"{data.Left.VariableName} == {data.Right.Variable}");
        var query = new Query<LeftJoinResult<VariableMessage, InlineData>>
            .Scan()
            .Interval(TestData.Interval)
            .Filter(type => type.And(
                type.Selector<>(
                    join => join.Left.VariableName,
                    pmPAct),
                type.Selector<>(
                    join => join.Left.TenantId,
                    tenantId)));
        var json = join.MapQueryToJson(query);
        var results = await join
            .ExecuteQuery(query)
            .ToListAsync();
    }

    [Test]
    public async Task Inline_Works()
    {
        var query = new Query<InlineData>
            .Scan()
            .Interval(new(default, DateTimeOffset.MaxValue))
            .Filter(type => type.In(
                data => data.Variable,
                new[] { "zero" }));
        var inline = Wikipedia
            .Inline(new InlineData[]
            {
                new("zero", "hello!"),
                new("one", "goodbye!")
            });
        var result = await inline
            .ExecuteQuery(query)
            .ToListAsync();
        result
            .Single()
            .Value
            .Should()
            .Be(new InlineData("zero", "hello!"));
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
            .Granularity(Granularity.All);

        var second10CountsPerCountry = countsPerCountry
            .LimitSpec(limit: 10, offset: 10, limit => limit.OrderBy(edit => edit.Dimensions, OrderDirection.Descending));
        await VerifyMatch(second10CountsPerCountry);

        var countsPerValidCountry = countsPerCountry
            .Filter(type => type.Not(type.Selector(edit => edit.CountryName, string.Empty)));
        await VerifyMatch(second10CountsPerCountry);
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
            .Context(new() { SkipEmptyBuckets = true });
        await VerifyMatch(lineStatisticsPerHour);
    }

    [Test]
    public async Task LatestForecastQuery_Works()
    {
        var query = new Query<VariableMessage>
            .GroupBy<Variable>
            .WithVirtualColumns<DateTimeOffset>
            .WithAggregations<LatestForecast>()
            .Interval(Interval)
            .Filter(filter => filter.And(
                filter.Selector(
                    data => data.Source.VariableName,
                    "pmPAct"),
                filter.Selector(
                    data => data.Source.TenantId,
                    tenantId)))
            .VirtualColumns(type => type.Expression<DateTimeOffset>(message => $"timestamp({message.ProcessedTimestamp})"))
            .Dimensions(type => new(
                type.Default(data => data.Source.ObjectId),
                type.Default(data => data.Source.VariableName)))
            .Aggregations(type => new(
                type.Max(data => data.VirtualColumns),
                type.Last(data => data.Source.Value, data => data.VirtualColumns)))
            .Granularity(Granularity.Hour);
        var json = Wikipedia
            .Edits
            .MapQueryToJson(query);
        var result = await Wikipedia
            .Edits
            .ExecuteQuery(query)
            .ToListAsync();
        result.Should().NotBeEmpty();
    }

    public sealed record Aggregations(double Sum, int Count, string Variable, double? FirstValue);
    public sealed record PostAggregations(double Average);
    public sealed record GroupByDimensions(Guid ObjectId, string VariableName);
    public sealed record InlineData(string Variable, [property: DataSourceColumn("MessageOfTheNight")] string MessageOfTheDay);
    public sealed record LatestForecast(DateTimeOffset Timestamp, double Value);
    public sealed record Variable(Guid ObjectId, string Name);
}
