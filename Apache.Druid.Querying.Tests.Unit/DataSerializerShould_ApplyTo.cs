using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Json;
using Snapshooter.NUnit;

namespace Apache.Druid.Querying.Tests.Unit;
internal class DataSerializerShould_ApplyTo
{
    private static void Verify(IQueryWith.Source<Data> query)
    {
        var json = query
            .MapToJson()
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
            DefaultSerializerOptions.Query,
            DefaultSerializerOptions.Data,
            () => throw new NotSupportedException()));

        var query = new Query<Inline_Data>.Scan();
        var json = provider
            .Inline(new[] { new Inline_Data(t, t.UtcDateTime, true, 1.5) })
            .MapQueryToJson(query)
            .ToString();
        Snapshot.Match(json);
    }
}
