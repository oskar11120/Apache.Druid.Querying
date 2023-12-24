using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Druid.Querying.Elements;

namespace Apache.Druid.Querying.Unit.Tests
{
    public class Tests
    {
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
    }

    internal static class TestExtensions
    {
        public static string ToJson(this IQuery query)
        {
            var asDictionary = query
                .GetState()
                .ToDictionary(pair => pair.Key, pair => pair.Value.Value);
            return JsonSerializer.Serialize<object>(asDictionary, new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                WriteIndented = true,
                Converters =
                {
                    new JsonStringEnumConverter(),
                    new PolymorphicSerializer<Filter>(),
                    new PolymorphicSerializer<Aggregator>(),
                    new PolymorphicSerializer<PostAggregator>(),
                    new PolymorphicSerializer<VirtualColumn>()
                }
            });
        }

        public class PolymorphicSerializer<T> : JsonConverter<T> where T : class
        {
            public override T Read(
                ref Utf8JsonReader reader,
                Type typeToConvert,
                JsonSerializerOptions options)
            {
                throw new NotImplementedException();
            }

            public override void Write(
                Utf8JsonWriter writer,
                [DisallowNull] T value,
                JsonSerializerOptions options)
            {
                JsonSerializer.Serialize(writer, value, value.GetType(), options);
            }
        }
    }
}