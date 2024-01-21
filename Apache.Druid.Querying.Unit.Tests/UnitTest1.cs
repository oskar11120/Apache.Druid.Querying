using Apache.Druid.Querying.Json;

namespace Apache.Druid.Querying.Unit.Tests
{
    public class Tests
    {
        [Test]
        public void Scan_Builds()
        {
            var zero = new Query<Message>
                .Scan
                .WithColumns<ScanColumns>()
                .Limit(10000)
                .BatchSize(2000)
                .Offset(4000)
                .Filter(type => type.Range(
                    columns => columns.Value,
                    lower: 100))
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(OrderDirection.Ascending)
                .MapToJson();
        }

        [Test]
        public void GroupByQuery_Builds()
        {
            var zero = new Query<Message>
                .GroupBy<GroupByDimensions>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Granularity(Granularity.Minute)
                .Dimensions(type => new
                (
                    type.Default(message => message.ObjectId),
                    type.Default(message => message.VariableName)
                ))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .LimitSpec(
                    5000,
                    columns: type => new[]
                    {
                        type.OrderBy(data => data.Dimensions.ObjectId),
                        type.OrderBy(data => data.Aggregations.LastValue)
                    })
                .HavingFilter(type => type.Range(
                    data => data.Aggregations.LastValue,
                    lower: 0))
                .MapToJson();

            var one = new Query<Message>
                .GroupBy<GroupByDimensions>
                .WithVirtualColumns<DateTimeOffset>
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Granularity(Granularity.Minute)
                .VirtualColumns(type => type.Expression<DateTimeOffset>(message => $"{message.Timestamp}"))
                .Dimensions(type => new
                (
                    type.Default(message => message.Source.ObjectId),
                    type.Default(message => message.Source.VariableName)
                ))
                .Aggregations(type => new(
                    type.Max(message => message.Source.Timestamp),
                    type.Last(message => message.Source.Value)
                ))
                .LimitSpec(
                    5000,
                    columns: type => new[]
                    {
                        type.OrderBy(data => data.Dimensions.ObjectId),
                        type.OrderBy(data => data.Aggregations.LastValue)
                    })
                .HavingFilter(type => type.Range(
                    data => data.Aggregations.LastValue,
                    lower: 0))
                .MapToJson();
        }

        [Test]
        public void TopNQuery_Builds()
        {
            var zero = new Query<Message>
                .TopN<Guid>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Dimension(type => type.Default(message => message.ObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .Metric(type => type.Numeric(
                    data => data.Aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 })
                .MapToJson();

            var one = new Query<Message>
                .TopN<Guid>
                .WithVirtualColumns<DateTimeOffset>
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .VirtualColumns(type => type.Expression<DateTimeOffset>(_ => $"__time"))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns),
                    type.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Dimension(type => type.Default(
                    message => message.Source.ObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ))
                .Metric(type => type.Numeric(
                    data => data.Aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 })
                .MapToJson();

            var two = new Query<Message>
                .TopN<TopNDimension>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<double>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Dimension(type => new(type.Default(
                    message => message.ObjectId)))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .PostAggregations(type => type.Arithmetic(
                    ArithmeticFunction.Add,
                    type.FieldAccess(type => type.LastValue, true)))
                .Metric(type => type.Numeric(data => data.PostAggregations))
                .Context(new() { MinTopNThreshold = 5 })
                .MapToJson();

            var three = new Query<Message>
                .TopN<TopNDimension>
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .VirtualColumns(type => new(type.Expression<DateTimeOffset>(_ => "__time")))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns.TReal),
                    type.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Dimension(type => new(
                    type.Default(message => message.Source.ObjectId)))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ))
                .PostAggregations(type => new(
                    type.Arithmetic(
                        ArithmeticFunction.Add,
                        type.FieldAccess(type => type.LastValue, true))))
                .Metric(type => type.Numeric(
                    data => data.PostAggregations.Sum))
                .Context(new() { MinTopNThreshold = 5 })
                .MapToJson();
        }

        [Test]
        public void TimeSeriesQuery_Builds()
        {
            var test0 = new Query<Message>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<double>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(type => type.Last(message => message.Value, SimpleDataType.Float))
                .MapToJson();

            var test1 = new Query<Message>
                .TimeSeries
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .VirtualColumns(type => new(type.Expression<DateTimeOffset>(_ => "__time")))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns.TReal),
                    type.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ))
                .MapToJson();

            var test2 = new Query<Message>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .PostAggregations(type => new
                (
                    type.Arithmetic(
                        ArithmeticFunction.Add,
                        type.FieldAccess(type => type.LastValue, true))
                ))
                .MapToJson();

            var test3 = new Query<Message>
                .TimeSeries
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .VirtualColumns(type => new(type.Expression<DateTimeOffset>(_ => "__time")))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns.TReal),
                    type.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ))
                .PostAggregations(type => new
                (
                    type.Arithmetic(
                        ArithmeticFunction.Add,
                        type.FieldAccess(type => type.LastValue, true))
                ))
                .MapToJson();
        }

        [DataSourceColumnNamingConvention(DataSourceColumnNamingConventionType.CamelCase)]
        internal record Message(
            [property: DataSourceColumn("variable")] string VariableName,
            Guid ObjectId,
            double Value,
            [property: DataSourceTimeColumn] DateTimeOffset Timestamp);
        record VirtualColumns(DateTimeOffset TReal);
        record Aggregations(DateTimeOffset TMax, double LastValue);
        record PostAggregations(double Sum);
        record TopNDimension(Guid ObjectId);
        record GroupByDimensions(Guid ObjectId, string VariableName);
        record ScanColumns(string VariableName, DateTimeOffset Timestamp, double Value);
    }
}