using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Druid.Querying.DependencyInjection;

namespace Apache.Druid.Querying.Unit.Tests
{
    public class Tests
    {
        [Test]
        public void GroupByQuery_Builds()
        {
            var zero = new Query<Message>
                .GroupBy<GroupByDimensions>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Granularity(Granularity.Minute)
                .Dimensions(dimensions => new
                (
                    dimensions.Default(message => message.ObjectId),
                    dimensions.Default(message => message.VariableName)
                ))
                .Aggregations(aggregators => new(
                    aggregators.Max(message => message.Timestamp),
                    aggregators.Last(message => message.Value)
                ))
                .LimitSpec(
                    5000,
                    columns: columns => new[]
                    {
                        columns.OrderBy(tuple => tuple.Dimensions.ObjectId),
                        columns.OrderBy(tuple => tuple.Aggregations.LastValue)
                    })
                .HavingFilter(filter => filter.Range(
                    tuple => tuple.Aggregations.LastValue,
                    lower: 0))
                .ToJson();

            var one = new Query<Message>
                .GroupBy<GroupByDimensions>
                .WithVirtualColumns<DateTimeOffset>
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Granularity(Granularity.Minute)
                .VirtualColumns(columns => columns.Expression<DateTimeOffset>("__time"))
                .Dimensions(dimensions => new
                (
                    dimensions.Default(message => message.Source.ObjectId),
                    dimensions.Default(message => message.Source.VariableName)
                ))
                .Aggregations(aggregators => new(
                    aggregators.Max(message => message.Source.Timestamp),
                    aggregators.Last(message => message.Source.Value)
                ))
                .LimitSpec(
                    5000,
                    columns: columns => new[]
                    {
                        columns.OrderBy(tuple => tuple.Dimensions.ObjectId),
                        columns.OrderBy(tuple => tuple.Aggregations.LastValue)
                    })
                .HavingFilter(filter => filter.Range(
                    tuple => tuple.Aggregations.LastValue,
                    lower: 0))
                .ToJson();
        }

        [Test]
        public void TopNQuery_Builds()
        {
            var zero = new Query<Message>
                .TopN<Guid>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Dimension(dimension => dimension.Default(message => message.ObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Filter(filter => filter.Or(
                    filter.Null(message => message.Value),
                    filter.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregations => new(
                    aggregations.Max(message => message.Timestamp),
                    aggregations.Last(message => message.Value)
                ))
                .Metric(metric => metric.Numeric(
                    tuple => tuple.Aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 })
                .ToJson();

            var one = new Query<Message>
                .TopN<Guid>
                .WithVirtualColumns<DateTimeOffset>
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .VirtualColumns(columns => columns.Expression<DateTimeOffset>("__time"))
                .Filter(filter => filter.Or(
                    filter.Null(pair => pair.VirtualColumns),
                    filter.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Dimension(dimension => dimension.Default(
                    message => message.Source.ObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Aggregations(aggregations => new(
                    aggregations.Max(tuple => tuple.Source.Timestamp),
                    aggregations.Last(tuple => tuple.Source.Value)
                ))
                .Metric(metric => metric.Numeric(
                    tuple => tuple.Aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 })
                .ToJson();

            var two = new Query<Message>
                .TopN<TopNDimension>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<double>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Dimension(dimension => new(dimension.Default(
                    message => message.ObjectId)))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Filter(filter => filter.Or(
                    filter.Null(message => message.Value),
                    filter.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregations => new(
                    aggregations.Max(message => message.Timestamp),
                    aggregations.Last(message => message.Value)
                ))
                .PostAggregations(postAggregators => postAggregators.Arithmetic(
                    ArithmeticFunction.Add,
                    postAggregators.FieldAccess(aggregations => aggregations.LastValue, true)))
                .Metric(metric => metric.Numeric(tuple => tuple.PostAggregations))
                .Context(new() { MinTopNThreshold = 5 })
                .ToJson();

            var three = new Query<Message>
                .TopN<TopNDimension>
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .VirtualColumns(columns => new(columns.Expression<DateTimeOffset>("__time")))
                .Filter(filter => filter.Or(
                    filter.Null(pair => pair.VirtualColumns.TReal),
                    filter.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Dimension(dimension => new(
                    dimension.Default(message => message.Source.ObjectId)))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Aggregations(aggregations => new(
                    aggregations.Max(tuple => tuple.Source.Timestamp),
                    aggregations.Last(tuple => tuple.Source.Value)
                ))
                .PostAggregations(postAggregators => new(
                    postAggregators.Arithmetic(
                        ArithmeticFunction.Add,
                        postAggregators.FieldAccess(aggregations => aggregations.LastValue, true))))
                .Metric(metric => metric.Numeric(
                    tuple => tuple.PostAggregations.Sum))
                .Context(new() { MinTopNThreshold = 5 })
                .ToJson();
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
                .Filter(filter => filter.Or(
                    filter.Null(message => message.Value),
                    filter.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregations => aggregations.Last(message => message.Value, SimpleDataType.Float))
                .ToJson();

            var test1 = new Query<Message>
                .TimeSeries
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .VirtualColumns(columns => new(columns.Expression<DateTimeOffset>("__time")))
                .Filter(filter => filter.Or(
                    filter.Null(pair => pair.VirtualColumns.TReal),
                    filter.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregations => new(
                    aggregations.Max(tuple => tuple.Source.Timestamp),
                    aggregations.Last(tuple => tuple.Source.Value)
                ))
                .ToJson();

            var test2 = new Query<Message>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(filter => filter.Or(
                    filter.Null(message => message.Value),
                    filter.Equals(
                        message => message.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregations => new(
                    aggregations.Max(message => message.Timestamp),
                    aggregations.Last(message => message.Value)
                ))
                .PostAggregations(postAggregators => new
                (
                    postAggregators.Arithmetic(
                        ArithmeticFunction.Add,
                        postAggregators.FieldAccess(aggregations => aggregations.LastValue, true))
                ))
                .ToJson();

            var test3 = new Query<Message>
                .TimeSeries
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .VirtualColumns(columns => new(columns.Expression<DateTimeOffset>("__time")))
                .Filter(filter => filter.Or(
                    filter.Null(pair => pair.VirtualColumns.TReal),
                    filter.Equals(
                        pair => pair.Source.ObjectId,
                        Guid.NewGuid())))
                .Aggregations(aggregations => new(
                    aggregations.Max(tuple => tuple.Source.Timestamp),
                    aggregations.Last(tuple => tuple.Source.Value)
                ))
                .PostAggregations(postAggregators => new
                (
                    postAggregators.Arithmetic(
                        ArithmeticFunction.Add,
                        postAggregators.FieldAccess(aggregations => aggregations.LastValue, true))
                ))
                .ToJson();
        }

        [DataSourceColumnNamingConvention(DataSourceColumnNamingConventionType.CamelCase)]
        internal record Message(
            [property: DataSourceColumn("Variable")] string VariableName,
            Guid ObjectId,
            double Value,
            DateTimeOffset Timestamp);
        record VirtualColumns(DateTimeOffset TReal);
        record Aggregations(DateTimeOffset TMax, double LastValue);
        record PostAggregations(double Sum);
        record TopNDimension(Guid ObjectId);
        record GroupByDimensions(Guid ObjectId, string VariableName);
    }

    internal static class TestExtensions
    {
        public static string ToJson(this IQuery query)
        {
            var options = DefaultSerializerOptions.Create();
            options.WriteIndented = true;
            var asDictionary = query
                .GetState()
                .ToDictionary(pair => pair.Key, pair => pair.Value(options, IArgumentColumnNameProvider.Implementation<Tests.Message>.Singleton));
            return JsonSerializer.Serialize(asDictionary, options);
        }
    }
}