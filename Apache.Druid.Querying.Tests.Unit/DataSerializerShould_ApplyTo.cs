using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Json;
using FluentAssertions;
using Snapshooter.NUnit;
using System.Linq.Expressions;
using System.Text.Json;

namespace Apache.Druid.Querying.Tests.Unit;
internal class DataSerializerShould_ApplyTo
{
    private static readonly JsonSerializerOptions dataSerializerOptions = DefaultSerializerOptions
        .Data
        .Create()
        .SerializeBoolsAsNumbers()
        .SerializeDateTimeAndDateTimeOffsetAsUnixMiliseconds();
    private static void Verify(IQueryWith.Source<Data> query)
    {
        var json = query
            .MapToJson(dataSerializerOptions: dataSerializerOptions)
            .ToString();
        Snapshot.Match(json);
    }

    private static readonly DateTimeOffset t = DateTimeOffset.UnixEpoch.AddYears(30).AddDays(1).AddHours(1).AddMinutes(1);
    private sealed record Data(
        [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
        bool Boolean);

    private sealed record Aggregations(
        DateTimeOffset Time,
        bool Bool);
    
    [Test]
    public void ExpressionAggregation()
    {
        var query = new Query<Data>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>()
            .Aggregations(type => new Aggregations(
                type.Expression(t, data => $"max({data.Timestamp}, _acc)"),
                type.Expression(true, data => $"{data.Boolean} && _acc")));
        Verify(query);
    }

    [Test]
    public void Filters()
    {
        var query = new Query<Data>
            .Scan()
            .Filter(type => type.And(
                type.In(data => data.Boolean, new[] { true, false }),
                type.In(data => data.Timestamp, new[] { t, t }),
                type.Equals(data => data.Boolean, true),
                type.Equals(data => data.Timestamp, t),
                type.Range(data => data.Boolean, false, true),
                type.Range(data => data.Timestamp, t, t),
                type.Selector(data => data.Boolean, true),
                type.Selector(data => data.Timestamp, t),
                type.Bound(data => data.Boolean, false, true),
                type.Bound(data => data.Timestamp, t, t)));
        Verify(query);
    }

    [Test]
    public void DimensionMetric()
    {
        var query = new Query<Data>
            .TopN<double>()
            .Metric(type => type.Dimension(t));
        Verify(query);
    }
    
    private sealed record Activity(
        [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
        int Duration,
        int DomainID,
        int UserID
    );
    private sealed record ActivityDimensions(int DomainID);
    private sealed record ActivityAggregations(List<long> UserIds, int Duration);
    
    [Test]
    public void ExpressionAggregationWithCombine()
    {
        var query = new Query<Activity>
                .GroupBy<ActivityDimensions>
                .WithNoVirtualColumns
                .WithAggregations<ActivityAggregations>()
            .Dimensions(type => new ActivityDimensions(type.Default(activity => activity.DomainID)))
            .Aggregations(type => new ActivityAggregations(
                type.Expression<List<long>, string>(
                    "ARRAY<LONG>[]",
                    "__acc",
                    data => $"array_set_add(__acc, {data.UserID})",
                    data => $"array_set_add_all(__acc, {data.Duration})",
                    null,
                    null,
                    data => "ARRAY<LONG>[]",
                    true,
                    true,
                    false,
                    1024 * 10
                ),
                type.Sum(activity => activity.Duration))
            );
        
        var json = query
            .MapToJson(dataSerializerOptions: dataSerializerOptions)
            .ToString();
        
        Snapshot.Match(json);
    }

    private sealed record Inline_Data(
        [property: DataSourceTimeColumn] DateTimeOffset DataTimeOffset,
        DateTime DateTime,
        bool Boolean,
        double Number);
    private sealed class InlineDataSourceProvider : DataSourceProvider
    {
    }

    [Test]
    public void InlineData()
    {
        var provider = new InlineDataSourceProvider();
        (provider as IDataSourceInitializer).Initialize(new(
            DefaultSerializerOptions.Query.ReadOnlySingleton,
            dataSerializerOptions,
            () => throw new NotSupportedException()));

        var query = new Query<Inline_Data>.Scan();
        var json = provider
            .Inline(new[] { new Inline_Data(t, t.UtcDateTime, true, 1.5) })
            .MapQueryToJson(query)
            .ToString();
        Snapshot.Match(json);
    }

    [Test]
    public void DruidExpressionConstants()
    {
        var text = "text";
        var number = 1.5;
        var boolean = true;
        var columnMappings = PropertyColumnNameMapping.ImmutableBuilder.Create<Data>();
        Expression<QueryElementFactory<Data>.DruidExpression> factory = data => $"'{text}' {number} {boolean} {t}";
        var result = DruidExpression.Map(factory, columnMappings, dataSerializerOptions).Expression;
        result.Should().Be("'text' 1.5 1 946774860000");
    }
}
