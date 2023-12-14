namespace Apache.Druid.Querying.Unit.Tests
{
    public class Tests
    {
        [Test]
        public void Test1()
        {
            var test0 = new TimeSeriesQuery<Message>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(Order.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(filter => filter.Or(
                    filter.Null(message => message.Value),
                    filter.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregators => new[]
                {
                    aggregators.Last(
                        aggregations => aggregations.LastValue,
                        message => message.Value),
                    aggregators.Max(
                        aggregations => aggregations.TMax,
                        message => message.Timestamp)
                });

            var test1 = new TimeSeriesQuery<Message>
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(Order.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(filter => filter.Or(
                    filter.Null(pair => pair.VirtualColumns.TReal),
                    filter.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregators => new[]
                {
                    aggregators.Last(
                        aggregations => aggregations.LastValue,
                        pair => pair.Source.Value),
                    aggregators.Max(
                        aggregations => aggregations.TMax,
                        pair => pair.Source.Timestamp)
                });

            var test2 = new TimeSeriesQuery<Message>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(Order.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(filter => filter.Or(
                    filter.Null(message => message.Value),
                    filter.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregators => new[]
                {
                    aggregators.Last(
                        aggregations => aggregations.LastValue,
                        message => message.Value),
                    aggregators.Max(
                        aggregations => aggregations.TMax,
                        message => message.Timestamp)
                })
                .PostAggregations(postAggregators => new[]
                {
                    postAggregators.Arithmetic(
                        postAggregations => postAggregations.Sum,
                        ArithmeticFunction.Add,
                        postAggregators.FieldAccess(
                            aggregations => aggregations.LastValue,
                            finalizing: true))
                });

            var test3 = new TimeSeriesQuery<Message>
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(Order.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(filter => filter.Or(
                    filter.Null(pair => pair.VirtualColumns.TReal),
                    filter.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregators => new[]
                {
                    aggregators.Last(
                        aggregations => aggregations.LastValue,
                        pair => pair.Source.Value),
                    aggregators.Max(
                        aggregations => aggregations.TMax,
                        pair => pair.Source.Timestamp)
                })
                .PostAggregations(postAggregators => new[]
                {
                    postAggregators.Arithmetic(
                        postAggregations => postAggregations.Sum,
                        ArithmeticFunction.Add,
                        postAggregators.FieldAccess(
                            aggregations => aggregations.LastValue,
                            finalizing: true))
                });

        }

        record Message(string Variable, Guid ObjectId, double Value, DateTimeOffset Timestamp, DateTimeOffset ProcessedTimestmap);
        record VirtualColumns(DateTimeOffset TReal);
        record Aggregations(DateTimeOffset TMax, double LastValue);
        record PostAggregations(double Sum);
    }
}