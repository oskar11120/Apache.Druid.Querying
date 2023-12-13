namespace Apache.Druid.Querying.Unit.Tests
{
    public class Tests
    {
        [Test]
        public void Test1()
        {
            new TimeSeriesQuery<Message, Aggregations>()
                .WithInterval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .WithOrder(Order.Ascending)
                .WithFilter(filter => filter.Or(
                    filter.Null(message => message.Value),
                    filter.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .WithAggregators(aggregators => new[]
                {
                    aggregators.Last(
                        aggregations => aggregations.TMax,
                        message => message.Timestamp)
                });

        }

        record Message(string Variable, Guid ObjectId, double Value, DateTimeOffset Timestamp, DateTimeOffset ProcessedTimestmap);
        record VirtualColumns(DateTimeOffset TReal);
        record Aggregations(DateTimeOffset TMax, double LastValue);
    }
}