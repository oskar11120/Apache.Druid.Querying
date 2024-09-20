using Apache.Druid.Querying.Json;
using Snapshooter.NUnit;

namespace Apache.Druid.Querying.Tests.Unit;
internal class DataSerializerShould_ApplyTo
{
    private static readonly DateTimeOffset t = DateTimeOffset.UnixEpoch.AddYears(30).AddDays(1).AddHours(1).AddMinutes(1);

    private sealed record Data(
        [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
        bool Boolean);
    private sealed record Aggregations(
        DateTimeOffset Time, 
        bool Bool);

    [Test]
    public void IntialValue_OfExpressionAggregation()
    {
        var json = new Query<Data>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>()
            .Aggregations(type => new Aggregations(
                type.Expression(t, data => $"max({data.Timestamp}, _acc)"),
                type.Expression(true, data => $"{data.Boolean} && _acc")))
            .MapToJson()
            .ToString();
        Snapshot.Match(json);
    }
}
