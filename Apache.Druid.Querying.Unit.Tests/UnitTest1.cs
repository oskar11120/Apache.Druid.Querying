using System.Text.Json;
using Apache.Druid.Querying.DependencyInjection;

namespace Apache.Druid.Querying.Unit.Tests
{
    public class Tests
    {
        [Test]
        public void TopNQuery_Builds()
        {
            var zero = new Query<Message>
                .TopN<TopNDimension>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Dimension(dimension => dimension.Default(
                    message => message.ObjectId,
                    dimension => dimension.ObjectId))
                .Threshold(5)
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
                .Metric(metric => metric.Numeric(
                    dimension => dimension.ObjectId))
                .Metric(metric => metric.Numeric(
                    (dimension, aggregations) => aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 })
                .ToJson();

            var one = new Query<Message>
                .TopN<TopNDimension>
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>()
                .VirtualColumns(columns => new[]
                {
                    columns.Expression(
                        virtualColumns => virtualColumns.TReal,
                        "__time")
                })
                .Filter(filter => filter.Or(
                    filter.Null(pair => pair.VirtualColumns.TReal),
                    filter.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Dimension(dimension => dimension.Default(
                    message => message.Source.ObjectId,
                    dimension => dimension.ObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Aggregations(aggregators => new[]
                {
                    aggregators.Last(
                        aggregations => aggregations.LastValue,
                        pair => pair.Source.Value),
                    aggregators.Max(
                        aggregations => aggregations.TMax,
                        pair => pair.Source.Timestamp)
                })
                .Metric(metric => metric.Numeric(
                    dimension => dimension.ObjectId))
                .Metric(metric => metric.Numeric(
                    (dimension, aggregations) => aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 })
                .ToJson();

            var two = new Query<Message>
                .TopN<TopNDimension>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Dimension(dimension => dimension.Default(
                    message => message.ObjectId,
                    dimension => dimension.ObjectId))
                .Threshold(5)
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
                })
                .Metric(metric => metric.Numeric(
                    dimension => dimension.ObjectId))
                .Metric(metric => metric.Numeric(
                    (dimension, aggregations) => aggregations.LastValue))
                .Metric(metric => metric.Numeric(
                    (dimension, aggregations, postAggregations) => postAggregations.Sum))
                .Context(new() { MinTopNThreshold = 5 })
                .ToJson();

            var three = new Query<Message>
                .TopN<TopNDimension>
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .VirtualColumns(columns => new[]
                {
                    columns.Expression(
                        virtualColumns => virtualColumns.TReal,
                        "__time")
                })
                .Filter(filter => filter.Or(
                    filter.Null(pair => pair.VirtualColumns.TReal),
                    filter.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Dimension(dimension => dimension.Default(
                    message => message.Source.ObjectId,
                    dimension => dimension.ObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
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
                })
                .Metric(metric => metric.Numeric(
                    dimension => dimension.ObjectId))
                .Metric(metric => metric.Numeric(
                    (dimension, aggregations) => aggregations.LastValue))
                .Metric(metric => metric.Numeric(
                    (dimension, aggregations, postAggregations) => postAggregations.Sum))
                .Context(new() { MinTopNThreshold = 5 })
                .ToJson();
        }

        [Test]
        public void TimeSeriesQuery_Builds()
        {
            var test0 = new Query<Message>
                .TimeSeries
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
                })
                .ToJson();

            var test1 = new Query<Message>
                .TimeSeries
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(Order.Ascending)
                .Granularity(Granularity.Minute)
                .VirtualColumns(columns => new[]
                {
                    columns.Expression(
                        virtualColumns => virtualColumns.TReal,
                        "__time")
                })
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
                .ToJson();

            var test2 = new Query<Message>
                .TimeSeries
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
                })
                .ToJson();

            var test3 = new Query<Message>
                .TimeSeries
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(Order.Ascending)
                .Granularity(Granularity.Minute)
                .VirtualColumns(columns => new[]
                {
                    columns.Expression(
                        virtualColumns => virtualColumns.TReal,
                        "__time")
                })
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
                            aggregations => aggregations.LastValue))
                })
                .ToJson();

        }

        record Message(
            [property: DataSourceColumn("variable")] string VariableName,
            Guid ObjectId,
            double Value,
            DateTimeOffset Timestamp);
        record VirtualColumns(DateTimeOffset TReal);
        record Aggregations(DateTimeOffset TMax, double LastValue);
        record PostAggregations(double Sum);
        record TopNDimension(Guid ObjectId);
    }

    internal static class TestExtensions
    {
        public static string ToJson(this IQuery query)
        {
            var asDictionary = query
                .GetState()
                .ToDictionary(pair => pair.Key, pair => pair.Value.Value);
            var options = DefaultSerializerOptions.Create();
            options.WriteIndented = true;
            return JsonSerializer.Serialize<object>(asDictionary, options);
        }
    }
}