using Apache.Druid.Querying.Json;
using Snapshooter;
using Snapshooter.NUnit;
using System.Runtime.CompilerServices;

namespace Apache.Druid.Querying.Tests.Unit
{
    internal class QueryTests_MapsToRightJson
    {
        private static readonly DateTimeOffset t = DateTimeOffset.UnixEpoch.AddYears(30).AddDays(1).AddHours(1).AddMinutes(1);
        private static readonly Guid guid = Guid.Parse("e3af0803-3fc1-407c-9071-29c5f1cdc8d2");

        private static void Match<TSource>(IQueryWithSource<TSource> query, [CallerArgumentExpression(nameof(query))] string? snapshotNameExtension = null)
        {
            var json = query
                .MapToJson()
                .ToString();
            Snapshot.Match(json, new SnapshotNameExtension(snapshotNameExtension));
        }

        [Test]
        public void Scan()
        {
            var zero = new Query<Message>
                .Scan
                .WithColumns<ScanColumns>() //TODO map column names from Message to ScanColumns
                .Limit(10000)
                .BatchSize(2000)
                .Offset(4000)
                .Filter(type => type.Range(
                    columns => columns.Value,
                    lower: 100))
                .Interval(new(t, t))
                .Order(OrderDirection.Ascending);
            Match(zero);
        }

        [Test]
        public void GroupBy()
        {
            var zero = new Query<Message>
                .GroupBy<GroupByDimensions>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
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
                    lower: 0));
            Match(zero);

            var one = new Query<Message>
                .GroupBy<GroupByDimensions>
                .WithVirtualColumns<DateTimeOffset>
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
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
                    lower: 0));
            Match(one);
        }

        [Test]
        public void TopN()
        {
            var zero = new Query<Message>
                .TopN<Guid>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
                .Dimension(type => type.Default(message => message.ObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.ObjectId,
                        guid)))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .Metric(type => type.Numeric(
                    data => data.Aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 });
            Match(zero);

            var one = new Query<Message>
                .TopN<Guid>
                .WithVirtualColumns<DateTimeOffset>
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
                .VirtualColumns(type => type.Expression<DateTimeOffset>(_ => $"__time"))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns),
                    type.Equals(
                        pair => pair.Source.ObjectId,
                        guid)))
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
                .Context(new() { MinTopNThreshold = 5 });
            Match(one);

            var two = new Query<Message>
                .TopN<TopNDimension>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<double>()
                .Interval(new(t, t))
                .Dimension(type => new(type.Default(
                    message => message.ObjectId)))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.ObjectId,
                        guid)))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .PostAggregations(type => type.Arithmetic(
                    ArithmeticFunction.Add,
                    type.FieldAccess(type => type.LastValue, true)))
                .Metric(type => type.Numeric(data => data.PostAggregations))
                .Context(new() { MinTopNThreshold = 5 });
            Match(two);

            var three = new Query<Message>
                .TopN<TopNDimension>
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(t, t))
                .VirtualColumns(type => new(type.Expression<DateTimeOffset>(_ => "__time")))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns.TReal),
                    type.Equals(
                        pair => pair.Source.ObjectId,
                        guid)))
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
                .Context(new() { MinTopNThreshold = 5 });
            Match(three);
        }

        [Test]
        public void TimeSeries()
        {
            var zero = new Query<Message>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<double>()
                .Interval(new(t, t))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.ObjectId,
                        guid)))
                .Aggregations(type => type.Last(message => message.Value, SimpleDataType.Float));
            Match(zero);

            var one = new Query<Message>
                .TimeSeries
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .VirtualColumns(type => new(type.Expression<DateTimeOffset>(_ => "__time")))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns.TReal),
                    type.Equals(
                        pair => pair.Source.ObjectId,
                        guid)))
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ));
            Match(one);

            var two = new Query<Message>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(t, t))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.ObjectId,
                        guid)))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .PostAggregations(type => new
                (
                    type.Arithmetic(
                        ArithmeticFunction.Add,
                        type.FieldAccess(type => type.LastValue, true))
                ));
            Match(two);

            var three = new Query<Message>
                .TimeSeries
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(t, t))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .VirtualColumns(type => new(type.Expression<DateTimeOffset>(_ => "__time")))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns.TReal),
                    type.Equals(
                        pair => pair.Source.ObjectId,
                        guid)))
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ))
                .PostAggregations(type => new
                (
                    type.Arithmetic(
                        ArithmeticFunction.Add,
                        type.FieldAccess(type => type.LastValue, true))
                ));
            Match(three);
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