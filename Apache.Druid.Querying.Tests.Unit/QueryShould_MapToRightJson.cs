using Apache.Druid.Querying.Json;
using Snapshooter;
using Snapshooter.NUnit;
using System.Runtime.CompilerServices;

namespace Apache.Druid.Querying.Tests.Unit
{
    internal class QueryShould_MapToRightJson
    {
        private static readonly DateTimeOffset t = DateTimeOffset.UnixEpoch.AddYears(30).AddDays(1).AddHours(1).AddMinutes(1);
        private static readonly Guid guid = Guid.Parse("e3af0803-3fc1-407c-9071-29c5f1cdc8d2");

        private static void AssertMatch<TSource>(IQueryWithSource<TSource> query, [CallerArgumentExpression(nameof(query))] string? snapshotNameExtension = null)
        {
            var json = query
                .MapToJson()
                .ToString();
            Snapshot.Match(json, new SnapshotNameExtension(snapshotNameExtension));
        }

        private sealed record AggregationsFromTernary(
            double OnType,
            DateTimeOffset OnData,
            double OnType_Nested,
            DateTimeOffset OnData_Nested);
        [TestCase(0)]
        [TestCase(1)]
        public void TernaryOperatorsInExpressions(int value)
        {
            Func<bool> valueGreaterThanZero = () => value > 0;
            var query = new Query<IotMeasurement>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<AggregationsFromTernary>
                .WithPostAggregations<int>()
                .Aggregations(type => new(
                    value > 0 ? type.Max(data => data.Value) : type.Min(data => data.Value),
                    type.Last(data => valueGreaterThanZero() ? data.Timestamp : data.ProcessedTimestamp),
                    value > 0 ? 
                        (value == 1 ?
                            type.First(data => data.Value) :
                            type.Last(data => data.Value)) :
                        type.Min(data => data.Value),
                    type.Last(data => valueGreaterThanZero() ? 
                        (valueGreaterThanZero() ? data.Timestamp : data.ProcessedTimestamp) :
                        data.ProcessedTimestamp)))
                .PostAggregations(type => valueGreaterThanZero() ? type.Constant(1) : type.Constant(0));
            AssertMatch(query, string.Empty);
        }

        [Test]
        public void Granularity_()
        {
            var query = new Query<IotMeasurement>.TimeSeries();

            var zero = query.Granularity(Granularity.Second);
            AssertMatch(zero);

            var one = query.Granularity(Granularity.None, "utc");
            AssertMatch(one);

            var two = query.Granularity(Granularity.Minute, "utc");
            AssertMatch(two);

            var three = query.Granularity(Granularity.Hour, "utc", t);
            AssertMatch(three);

            var four = query.Granularity(TimeSpan.FromHours(1));
            AssertMatch(four);

            var five = query.Granularity(TimeSpan.FromHours(1), t);
            AssertMatch(five);

            var six = query.Granularity("T1M");
            AssertMatch(six);

            var seven = query.Granularity("T1M", "utc");
            AssertMatch(seven);

            var eight = query.Granularity("T1M", "utc", t);
            AssertMatch(eight);
        }

        [Test]
        public void Scan()
        {
            var zero = new Query<IotMeasurement>
                .Scan()
                .Limit(10000)
                .BatchSize(2000)
                .Offset(4000)
                .Filter(type => type.Range(
                    columns => columns.Value,
                    lower: 100))
                .Interval(new(t, t))
                .Order(OrderDirection.Ascending);
            AssertMatch(zero);
        }

        [Test]
        public void GroupBy()
        {
            var zero = new Query<IotMeasurement>
                .GroupBy<GroupByDimensions>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
                .Granularity(Granularity.Minute)
                .Dimensions(type => new
                (
                    type.Default(message => message.IotObjectId),
                    type.Default(message => message.SignalName)
                ))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .LimitSpec(
                    5000,
                    columns: type => new[]
                    {
                        type.OrderBy(data => data.Dimensions.IotObjectId),
                        type.OrderBy(data => data.Aggregations.LastValue)
                    })
                .HavingFilter(type => type.Range(
                    data => data.Aggregations.LastValue,
                    lower: 0));
            AssertMatch(zero);

            var one = new Query<IotMeasurement>
                .GroupBy<GroupByDimensions>
                .WithVirtualColumns<DateTimeOffset>
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
                .Granularity(Granularity.Minute)
                .VirtualColumns(type => type.Expression<DateTimeOffset>(message => $"{message.Timestamp}"))
                .Dimensions(type => new
                (
                    type.Default(message => message.Source.IotObjectId),
                    type.Default(message => message.Source.SignalName)
                ))
                .Aggregations(type => new(
                    type.Max(message => message.Source.Timestamp),
                    type.Last(message => message.Source.Value)
                ))
                .LimitSpec(
                    5000,
                    columns: type => new[]
                    {
                        type.OrderBy(data => data.Dimensions.IotObjectId),
                        type.OrderBy(data => data.Aggregations.LastValue)
                    })
                .HavingFilter(type => type.Range(
                    data => data.Aggregations.LastValue,
                    lower: 0));
            AssertMatch(one);
        }

        [Test]
        public void TopN()
        {
            var zero = new Query<IotMeasurement>
                .TopN<Guid>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
                .Dimension(type => type.Default(message => message.IotObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.IotObjectId,
                        guid)))
                .Aggregations(type => new(
                    type.Max(message => message.Timestamp),
                    type.Last(message => message.Value)
                ))
                .Metric(type => type.Numeric(
                    data => data.Aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 });
            AssertMatch(zero);

            var one = new Query<IotMeasurement>
                .TopN<Guid>
                .WithVirtualColumns<DateTimeOffset>
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
                .VirtualColumns(type => type.Expression<DateTimeOffset>(_ => $"__time"))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns),
                    type.Equals(
                        pair => pair.Source.IotObjectId,
                        guid)))
                .Dimension(type => type.Default(
                    message => message.Source.IotObjectId))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ))
                .Metric(type => type.Numeric(
                    data => data.Aggregations.LastValue))
                .Context(new() { MinTopNThreshold = 5 });
            AssertMatch(one);

            var two = new Query<IotMeasurement>
                .TopN<TopNDimension>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>
                .WithPostAggregations<double>()
                .Interval(new(t, t))
                .Dimension(type => new(type.Default(
                    message => message.IotObjectId)))
                .Threshold(5)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.IotObjectId,
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
            AssertMatch(two);

            var three = new Query<IotMeasurement>
                .TopN<TopNDimension>
                .WithVirtualColumns<VirtualColumns>
                .WithAggregations<Aggregations>
                .WithPostAggregations<PostAggregations>()
                .Interval(new(t, t))
                .VirtualColumns(type => new(type.Expression<DateTimeOffset>(_ => "__time")))
                .Filter(type => type.Or(
                    type.Null(pair => pair.VirtualColumns.TReal),
                    type.Equals(
                        pair => pair.Source.IotObjectId,
                        guid)))
                .Dimension(type => new(
                    type.Default(message => message.Source.IotObjectId)))
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
            AssertMatch(three);
        }

        [Test]
        public void TimeSeries()
        {
            var zero = new Query<IotMeasurement>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<double>()
                .Interval(new(t, t))
                .Order(OrderDirection.Ascending)
                .Granularity(Granularity.Minute)
                .Filter(type => type.Or(
                    type.Null(message => message.Value),
                    type.Equals(
                        message => message.IotObjectId,
                        guid)))
                .Aggregations(type => type.Last(message => message.Value, SimpleDataType.Float));
            AssertMatch(zero);

            var one = new Query<IotMeasurement>
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
                        pair => pair.Source.IotObjectId,
                        guid)))
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ));
            AssertMatch(one);

            var two = new Query<IotMeasurement>
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
                        message => message.IotObjectId,
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
            AssertMatch(two);

            var three = new Query<IotMeasurement>
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
                        pair => pair.Source.IotObjectId,
                        guid)))
                .Aggregations(type => new(
                    type.Max(data => data.Source.Timestamp),
                    type.Last(data => data.Source.Value)
                ))
                .PostAggregations(type => new(
                    type.Arithmetic(
                        ArithmeticFunction.Add,
                        type.FieldAccess(type => type.LastValue, true))
                ));
            AssertMatch(three);
        }

        [Test]
        public void WithDataPropertiesAccessedByInterface()
        {
            static void Test<TMeasurement>() where TMeasurement : IIotMeasurement
            {
                var one = new Query<TMeasurement>
                    .Scan()
                    .Interval(new(t, t))
                    .Filter(type => type.And(
                        type.Selector(data => data.ObjectId, Guid.Empty),
                        type.Selector(data => data.Value, 1)));
                AssertMatch(one);
            }

            Test<IotMeasurement>();
        }

        interface IIotMeasurement
        {
            Guid ObjectId { get; }
            double Value { get; }
        }

        [DataSourceColumnNamingConvention.CamelCase]
        internal record IotMeasurement(
            [property: DataSourceColumn("signal")] string SignalName,
            Guid IotObjectId,
            double Value,
            [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
            DateTimeOffset ProcessedTimestamp)
            : IIotMeasurement
        {
            [property: DataSourceColumn("iotObjectId")]
            Guid IIotMeasurement.ObjectId => IotObjectId;
        }

        record VirtualColumns(DateTimeOffset TReal);
        record Aggregations(DateTimeOffset TMax, double LastValue);
        record PostAggregations(double Sum);
        record TopNDimension(Guid IotObjectId);
        record GroupByDimensions(Guid IotObjectId, string SignalName);
    }
}