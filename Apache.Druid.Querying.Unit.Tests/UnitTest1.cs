namespace Apache.Druid.Querying.Unit.Tests
{
    public class Tests
    {
        [Test]
        public void Test1()
        {
            new TimeSeriesQuery<Message>()
                .WithInterval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .WithOrder(QueryOrder.Ascending)
                .WithFilter(filter => filter.Or(
                    filter.Null(message => message.Value),
                    filter.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())));

        }

        record Message(string Variable, Guid ObjectId, double Value, DateTimeOffset Timestamp, DateTimeOffset ProcessedTimestmap);
    }
}