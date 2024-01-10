using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Globalization;
using static Apache.Druid.Querying.AspNetCore.Tests.Integration.ServiceProvider;

namespace Apache.Druid.Querying.AspNetCore.Tests.Integration;

internal static class TestExtensions
{
    public static TQuery IntervalFilterDefaults<TQuery>(this TQuery query) where TQuery :
        IQueryWith.Intervals,
        IQueryWith.Filter<VariableMessage, TQuery>
    {
        var t = DateTime.Parse("2023-10-19T16:57:00.000Z", null, DateTimeStyles.AssumeUniversal).ToUniversalTime();
        return query
            .Interval(new(t, t.AddDays(5)))
            .Filter(filter => filter.And(
                filter.Selector(
                    message => message.VariableName,
                    "pmPAct"),
                filter.Selector(
                    message => message.TenantId,
                    Guid.Parse("55022f5d-d9c4-4773-86e5-fbce823cd287"))));
    }

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
    public async Task InlineWorks()
    {

        var query = new Query<InlineData>
            .Scan()
            .Filter(type => type.Range(
                data => data.Value,
                upper: 2,
                upperOpen: true));
        var inline = Druid
            .Inline(new InlineData[]
            {
                new(1, "one"),
                new(2, "two")
            });
        var result = await inline
            .ExecuteQuery(query)
            .ToListAsync();
        result
            .Single()
            .Value
            .Should()
            .Be(new InlineData(1, "one"));
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
        var result = await Druid
            .Variables
            .ExecuteQuery(query)
            .ToListAsync();
        result.Should().NotBeEmpty();
    }

    public sealed record Aggregations(double Sum, int Count, string Variable, double? FirstValue);
    public sealed record PostAggregations(double Average);
    public sealed record GroupByDimensions(Guid ObjectId, string VariableName);
    public sealed record InlineData(int Value, string MessageOfTheDay);
}
