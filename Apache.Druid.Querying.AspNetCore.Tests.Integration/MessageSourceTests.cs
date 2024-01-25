using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Globalization;
using static Apache.Druid.Querying.AspNetCore.Tests.Integration.ServiceProvider;
using static Apache.Druid.Querying.AspNetCore.Tests.Integration.TestData;

namespace Apache.Druid.Querying.AspNetCore.Tests.Integration;

internal static class TestData
{
    public const string pmPAct = nameof(pmPAct);
    public static readonly Guid tenantId = Guid.Parse("55022f5d-d9c4-4773-86e5-fbce823cd287");
    public static readonly DateTimeOffset t0 = DateTime.Parse("2024-01-20T16:57:00.000Z", null, DateTimeStyles.AssumeUniversal).ToUniversalTime();
    public static readonly Interval interval = new(t0, t0.AddDays(5));

    public static TQuery IntervalFilterDefaults<TQuery>(this TQuery query) where TQuery :
        IQueryWith.Intervals,
        IQueryWith.Filter<VariableMessage, TQuery>
        => query
            .Interval(interval)
            .Filter(filter => filter.And(
                filter.Selector(
                    message => message.VariableName,
                    "pmPAct"),
                filter.Selector(
                    message => message.TenantId,
                    tenantId)));

    public static TQuery AggregationsDefaults<TQuery>(this TQuery query) where TQuery :
        IQueryWith.Granularity,
        IQueryWith.Aggregations<VariableMessage, MessageSourceTests.Aggregations, TQuery>
        => query
            .Granularity(Granularity.SixHours)
            .Aggregations(factory => new(
                factory.Sum(message => message.Value),
                factory.Count(),
                factory.First(message => message.VariableName),
                factory.First(message => message.Value, SimpleDataType.String)
            ));
}

internal class MessageSourceTests
{
    private static EcDruid Druid => Services.GetRequiredService<EcDruid>();

    [Test]
    public async Task Query_Works()
    {
        var first = new Query<VariableMessage>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>()
            .IntervalFilterDefaults()
            .AggregationsDefaults();
        var firstSource = Druid
            .Variables
            .WrapOverQuery(first);
        var firstJson = firstSource.GetJsonRepresentation();
        var second = new Query<WithTimestamp<Aggregations>>
            .Scan()
            .Interval(interval)
            .Filter(type => type.Not(type.Selector(first => first.Value.Count, 6)));
        var secondJson = firstSource.MapQueryToJson(second);
        var results = await firstSource
            .ExecuteQuery(second)
            .ToListAsync();
    }

    [Test]
    public async Task Join_Works()
    {
        var inline = Druid
            .Inline(new InlineData[]
            {
                new("pmPAct", "hello!"),
                new("notPmPAct", "goodbye!")
            });
        var join = Druid
            .Variables
            .LeftJoin(inline, data => $"{data.Left.VariableName} == {data.Right.Variable}");
        var query = new Query<LeftJoinResult<VariableMessage, InlineData>>
            .Scan()
            .Interval(interval)
            .Filter(type => type.And(
                type.Selector(
                    join => join.Left.VariableName,
                    pmPAct),
                type.Selector(
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
        var inline = Druid
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
    public async Task Scan_ReturnsAnything()
    {
        var query = new Query<VariableMessage>
            .Scan()
            .IntervalFilterDefaults()
            .Limit(10000)
            .Offset(5)
            .Order(OrderDirection.Ascending);
        var result = await Druid
            .Variables
            .ExecuteQuery(query)
            .ToListAsync();
        result.Should().NotBeEmpty();
    }

    [Test]
    public async Task GroupBy_ReturnsAnything()
    {
        var query = new Query<VariableMessage>
            .GroupBy<GroupByDimensions>
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>
            .WithPostAggregations<double>()
            .IntervalFilterDefaults()
            .AggregationsDefaults()
            .PostAggregations(factory => factory.Arithmetic(
                ArithmeticFunction.Divide,
                factory.FieldAccess(aggrgations => aggrgations.Sum),
                factory.FieldAccess(aggregations => aggregations.Count)))
            .Dimensions(factory => new(
                factory.Default(message => message.ObjectId),
                factory.Default(message => message.VariableName)))
            .LimitSpec(1000);
        var result = await Druid
            .Variables
            .ExecuteQuery(query)
            .ToListAsync();
        result.Should().NotBeEmpty();
    }

    [Test]
    public async Task TopN_ReturnsAnything()
    {
        var query = new Query<VariableMessage>
            .TopN<Guid>
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>
            .WithPostAggregations<PostAggregations>()
            .IntervalFilterDefaults()
            .AggregationsDefaults()
            .PostAggregations(factory =>
                new(factory.Arithmetic(
                    ArithmeticFunction.Divide,
                    factory.FieldAccess(aggrgations => aggrgations.Sum),
                    factory.FieldAccess(aggregations => aggregations.Count))))
            .Dimension(factory => factory.Default(message => message.ObjectId))
            .Metric(factory => factory.Numeric(tuple => tuple.Aggregations.Count))
            .Threshold(1000);
        var result = await Druid
            .Variables
            .ExecuteQuery(query)
            .ToListAsync();
        result.Should().NotBeEmpty();
    }

    [Test]
    public async Task TimeSeries_ReturnsAnything()
    {
        var query = new Query<VariableMessage>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>
            .WithPostAggregations<PostAggregations>()
            .IntervalFilterDefaults()
            .AggregationsDefaults()
            .PostAggregations(factory => new(
                factory.Arithmetic(
                    ArithmeticFunction.Divide,
                    factory.FieldAccess(aggrgations => aggrgations.Sum),
                    factory.FieldAccess(aggregations => aggregations.Count))))
            .Context(new() { SkipEmptyBuckets = true });
        var json = Druid
            .Variables
            .MapQueryToJson(query);
        var result = await Druid
            .Variables
            .ExecuteQuery(query)
            .ToListAsync();
        result.Should().NotBeEmpty();
    }

    [Test]
    public async Task LatestForecastQuery_Works()
    {
        var query = new Query<VariableMessage>
            .GroupBy<Variable>
            .WithVirtualColumns<DateTimeOffset>
            .WithAggregations<LatestForecast>()
            .Interval(interval)
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
        var json = Druid
            .Variables
            .MapQueryToJson(query);
        var result = await Druid
            .Variables
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
