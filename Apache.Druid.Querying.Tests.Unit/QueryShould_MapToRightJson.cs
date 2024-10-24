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

        private static void AssertMatch<TSource>(IQueryWith.Source<TSource> query, [CallerArgumentExpression(nameof(query))] string? snapshotNameExtension = null)
        {
            var json = query
                .MapToJson()
                .ToString();
            Snapshot.Match(json, new SnapshotNameExtension(snapshotNameExtension));
        }

        private sealed record FilteredAggregations(double First);
        [Test]
        public void FilteredAggregation()
        {
            var first = new Query<IotMeasurement>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<double>()
                .Aggregations(type => type.Filtered(
                    filterType => filterType.Selector(data => data.Value, 1),
                    type.First(data => data.Value)));
            AssertMatch(first);

            var second = new Query<IotMeasurement>
                .TimeSeries
                .WithNoVirtualColumns
                .WithAggregations<FilteredAggregations>()
                .Aggregations(type => new(type.Filtered(
                    filterType => filterType.Selector(data => data.Value, 1),
                    type.First(data => data.Value))));
            AssertMatch(second);
        }

        private sealed record NonePostAggregations(long Value);
        [Test]
        public void NoneElementType()
        {
            static Query<IotMeasurement>
                .GroupBy<double>
                .WithVirtualColumns<int>
                .WithAggregations<string>
                .WithPostAggregations<NonePostAggregations> Query()
                => new();
            var dimensions = Query().Dimensions(type => type.None<double>());
            AssertMatch(dimensions);
            var virtualColumns = Query().VirtualColumns(type => type.None<int>());
            AssertMatch(virtualColumns);
            var aggregations = Query().Aggregations(type => type.None<string>());
            AssertMatch(aggregations);
            var postAggregations = Query().PostAggregations(type => new(type.None<long>()));
            AssertMatch(postAggregations);
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

            var zero = query.Granularity(SimpleGranularity.Second);
            AssertMatch(zero);

            var one = query.Granularity(SimpleGranularity.None, "utc");
            AssertMatch(one);

            var two = query.Granularity(SimpleGranularity.Minute, "utc");
            AssertMatch(two);

            var three = query.Granularity(SimpleGranularity.Hour, "utc", t);
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
        
        private sealed record Activity(
            [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
            int Duration,
            int DomainID,
            int UserID
        );
        private sealed record ActivityDimensions(int DomainID);
        private sealed record ActivityAggregations(List<long> UserIds, int Duration);
    
        [Test]
        public void ExpressionAggregationWithCombine()
        {
            var query = new Query<Activity>
                    .GroupBy<ActivityDimensions>
                    .WithNoVirtualColumns
                    .WithAggregations<ActivityAggregations>()
                .Dimensions(type => new ActivityDimensions(type.Default(activity => activity.DomainID)))
                .Aggregations(type => new ActivityAggregations(
                    type.Expression<List<long>, string>(
                        "ARRAY<LONG>[]",
                        "__acc",
                        data => $"array_set_add(__acc, {data.UserID})",
                        data => $"array_set_add_all(__acc, {data.Duration})",
                        null,
                        null,
                        data => "ARRAY<LONG>[]",
                        true,
                        true,
                        false,
                        1024 * 10
                    ),
                    type.Sum(activity => activity.Duration))
                );
            AssertMatch(query);
        }

        private record IotMeasurementTimestamps(DateTimeOffset Normal, DateTimeOffset Processed);
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

            var one = new Query<IotMeasurement>
                .Scan
                .WithColumns<IotMeasurementTimestamps>()
                .Limit(10000)
                .BatchSize(2000)
                .Offset(4000)
                .Filter(type => type.Range(
                    columns => columns.Value,
                    lower: 100))
                .Interval(new(t, t))
                .Columns(measurement => new IotMeasurementTimestamps(measurement.Timestamp, measurement.ProcessedTimestamp));
            AssertMatch(one);
        }

        [Test]
        public void GroupBy()
        {
            var zero = new Query<IotMeasurement>
                .GroupBy<GroupByDimensions>
                .WithNoVirtualColumns
                .WithAggregations<Aggregations>()
                .Interval(new(t, t))
                .Granularity(SimpleGranularity.Minute)
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
                .Granularity(SimpleGranularity.Minute)
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
                .Granularity(SimpleGranularity.Minute)
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
                .Granularity(SimpleGranularity.Minute)
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
                .Granularity(SimpleGranularity.Minute)
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
                .Granularity(SimpleGranularity.Minute)
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
                .Granularity(SimpleGranularity.Minute)
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
                .Descending(false)
                .Granularity(SimpleGranularity.Minute)
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
                .Granularity(SimpleGranularity.Minute)
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
                .Descending(false)
                .Granularity(SimpleGranularity.Minute)
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

        [Test]
        public void OnMapToJson()
        {
            var zero = new Query<IotMeasurement>
                .Scan()
                .Interval(new(t, t))
                .OnMapToJson((_, json) => json.Add("first", 1));
            var one = zero
                .Copy()
                .OnMapToJson((_, json) => json.Add("second", 2));
            AssertMatch(zero);
            AssertMatch(one);
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