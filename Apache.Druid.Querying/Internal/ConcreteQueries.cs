using Apache.Druid.Querying.Internal.QuerySectionFactory;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Druid.Querying.Internal
{
    public static class IQueryWithMappedResult
    {
        public interface ArrayOfObjectsWithTimestamp<TResult, TResultMapper>
            : IQueryWithMappedResult<WithTimestamp<TResult>,
            QueryResultMapper.Array<WithTimestamp<TResult>, QueryResultMapper.WithTimestamp<TResult, TResultMapper>>>
            where TResultMapper : IQueryResultMapper<TResult>, new()
        {
        }

        public static class WithTimestamp
        {
            public interface Aggregations_PostAggregations_<TAggregations, TPostAggregations> : ArrayOfObjectsWithTimestamp<
                Aggregations_PostAggregations<TAggregations, TPostAggregations>,
                QueryResultMapper.Aggregations_PostAggregations_<TAggregations, TPostAggregations>>
            {
            }

            public interface Dimension_Aggregations_<TDimension, TAggregations> : ArrayOfObjectsWithTimestamp<
                Dimension_Aggregations<TDimension, TAggregations>,
                QueryResultMapper.Array<
                    Dimension_Aggregations<TDimension, TAggregations>,
                    QueryResultMapper.Dimension_Aggregations_<TDimension, TAggregations>>>
            {
            }

            public interface Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations> : ArrayOfObjectsWithTimestamp<
                Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>,
                QueryResultMapper.Array<
                    Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>,
                    QueryResultMapper.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>>>
            {
            }
        }
    }

    public static class QueryResultMapper
    {
        private static readonly byte[] comaBytes = Encoding.UTF8.GetBytes(",");

        private static TObject Deserialize<TObject>(JsonStreamReader json, JsonSerializerOptions options)
        {
            json.ReadToTokenTypeAtNextTokenDepth(JsonTokenType.EndObject, out var bytesConsumed, false);
            var startWithComa = json.UnreadPartOfBufferStartsWith(comaBytes);   
            var reader = json.GetReaderForSlice((int)bytesConsumed, startWithComa ? comaBytes.Length : 0);
            var result = JsonSerializer.Deserialize<TObject>(ref reader, options)!;
            return result;
        }

        private static TObject DeserializeAndUpdateState<TObject>(JsonStreamReader json, JsonSerializerOptions options)
        {
            json.ReadToTokenTypeAtNextTokenDepth(JsonTokenType.EndObject, out var bytesConsumed, false);
            var startWithComa = json.UnreadPartOfBufferStartsWith(comaBytes);
            var trimBytes = startWithComa ? comaBytes.Length : 0;
            var sliceReader = json.GetReaderForSlice((int)bytesConsumed, trimBytes);
            var result = JsonSerializer.Deserialize<TObject>(ref sliceReader, options)!;
            var updateReader = json.GetReader();
            while (updateReader.BytesConsumed < sliceReader.BytesConsumed + trimBytes)
                updateReader.Read();
            json.UpdateState(updateReader);
            return result;
        }

        private static async ValueTask<int> EnsureWholeObjectInBufferAndGetBytesConsumedAsync(JsonStreamReader json, CancellationToken token)
        {
            long bytesConsumed;
            while (!json.ReadToTokenTypeAtNextTokenDepth(JsonTokenType.EndObject, out bytesConsumed, false))
                await json.AdvanceAsync(token);
            return (int)bytesConsumed;
        }

        public class WithTimestamp<TResult, TResultMapper> :
            IQueryResultMapper<WithTimestamp<TResult>>
            where TResultMapper : IQueryResultMapper<TResult>, new()
        {
            private static byte[] ToJson(string propertyName) =>
                Encoding.UTF8.GetBytes(propertyName.ToCamelCase());

            private static readonly IQueryResultMapper<TResult> mapper = new TResultMapper();
            private static readonly (byte[] Timestamp, byte[] Result) names = (
                ToJson(nameof(WithTimestamp<TResult>.Timestamp)),
                ToJson(nameof(WithTimestamp<TResult>.Result)));

            async IAsyncEnumerable<WithTimestamp<TResult>> IQueryResultMapper<WithTimestamp<TResult>>.Map(
                JsonStreamReader json, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                DateTimeOffset t;
                while (!json.ReadToPropertyValue(names.Timestamp, out t))
                    await json.AdvanceAsync(token);
                while (!json.ReadToProperty(names.Result))
                    await json.AdvanceAsync(token);

                var results = mapper.Map(json, options, token);
                await foreach (var result in results)
                    yield return new(t, result);

                while (!json.ReadToTokenType(JsonTokenType.EndObject))
                    await json.AdvanceAsync(token);
            }
        }

        public class Array<TElement, TElementMapper> :
            IQueryResultMapper<TElement>
            where TElementMapper : IQueryResultMapper<TElement>, new()
        {
            private static readonly IQueryResultMapper<TElement> mapper = new TElementMapper();

            async IAsyncEnumerable<TElement> IQueryResultMapper<TElement>.Map(
                JsonStreamReader json, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                bool SkipEndArray(out bool skippedEndArray)
                {
                    var reader = json.GetReader();
                    var read = reader.Read();
                    if (!read)
                    {
                        skippedEndArray = default;
                        return false;
                    }

                    skippedEndArray = reader.TokenType is JsonTokenType.EndArray;
                    if (skippedEndArray)
                        json.UpdateState(reader);
                    return true;
                }

                while (!json.ReadToTokenType(JsonTokenType.StartArray))
                    await json.AdvanceAsync(token);
                while (true)
                {
                    bool skippedEndArray;
                    while (!SkipEndArray(out skippedEndArray))
                        await json.AdvanceAsync(token);
                    if (skippedEndArray)
                        yield break;

                    var results = mapper.Map(json, options, token);
                    await foreach (var result in results)
                        yield return result;

                    while (!SkipEndArray(out skippedEndArray))
                        await json.AdvanceAsync(token);
                    if (skippedEndArray)
                        yield break;
                }
            }
        }

        public class Aggregations_PostAggregations_<TAggregations, TPostAggregations> :
            IQueryResultMapper<Aggregations_PostAggregations<TAggregations, TPostAggregations>>
        {
            async IAsyncEnumerable<Aggregations_PostAggregations<TAggregations, TPostAggregations>> IQueryResultMapper<Aggregations_PostAggregations<TAggregations, TPostAggregations>>.Map(
                JsonStreamReader json, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                await EnsureWholeObjectInBufferAndGetBytesConsumedAsync(json, token);
                yield return new(
                    Deserialize<TAggregations>(json, options),
                    DeserializeAndUpdateState<TPostAggregations>(json, options));
            }
        }

        public class Dimension_Aggregations_<TDimension, TAggregations> :
            IQueryResultMapper<Dimension_Aggregations<TDimension, TAggregations>>
        {
            async IAsyncEnumerable<Dimension_Aggregations<TDimension, TAggregations>> IQueryResultMapper<Dimension_Aggregations<TDimension, TAggregations>>.Map(
                JsonStreamReader json, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                await EnsureWholeObjectInBufferAndGetBytesConsumedAsync(json, token);
                yield return new(
                    Deserialize<TDimension>(json, options),
                    DeserializeAndUpdateState<TAggregations>(json, options));
            }
        }

        public class Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
             : IQueryResultMapper<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>
        {
            async IAsyncEnumerable<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>> IQueryResultMapper<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>.Map(
                JsonStreamReader json, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                await EnsureWholeObjectInBufferAndGetBytesConsumedAsync(json, token);
                yield return new(
                    Deserialize<TDimension>(json, options),
                    Deserialize<TAggregations>(json, options),
                    DeserializeAndUpdateState<TPostAggregations>(json, options));
            }
        }

        public class Dimensions_Aggregations_<TDimensions, TAggregations>
            : IQueryResultMapper<Dimensions_Aggregations<TDimensions, TAggregations>>
        {
            async IAsyncEnumerable<Dimensions_Aggregations<TDimensions, TAggregations>> IQueryResultMapper<Dimensions_Aggregations<TDimensions, TAggregations>>.Map(
                JsonStreamReader json, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                await EnsureWholeObjectInBufferAndGetBytesConsumedAsync(json, token);
                yield return new(
                    Deserialize<TDimensions>(json, options),
                    DeserializeAndUpdateState<TAggregations>(json, options));
            }
        }

        public class Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
             : IQueryResultMapper<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
        {
            async IAsyncEnumerable<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>> IQueryResultMapper<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>.Map(
                JsonStreamReader json, JsonSerializerOptions options, [EnumeratorCancellation] CancellationToken token)
            {
                await EnsureWholeObjectInBufferAndGetBytesConsumedAsync(json, token);
                yield return new(
                    Deserialize<TDimensions>(json, options),
                    Deserialize<TAggregations>(json, options),
                    DeserializeAndUpdateState<TPostAggregations>(json, options));
            }
        }
    }

    public static class Marker
    {
        public sealed record Dimension;
        public sealed record Dimensions;
    }

    public static class QueryBase<TArguments, TSelf> where TSelf : IQuery<TSelf>
    {
        public abstract class TimeSeries :
            Query,
            IQueryWith.Order,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TimeSeries, TSelf>
        {
            public TimeSeries() : base("timeseries")
            {
            }
        }

        public abstract class TimeSeries<TAggregations> :
            TimeSeries,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class TimeSeries<TAggregations, TPostAggregations> :
            TimeSeries<TAggregations>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }

        private static readonly SectionFactoryJsonMapper.Options dimensionsMapperOptions = new(SectionColumnNameKey: "outputName");
        public abstract class TopN_<TDimension, TMetricArguments> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>,
            IQuery<TArguments, TSelf, Marker.Dimension>
        {
            private static readonly SectionFactoryJsonMapper.Options mapperOptions = dimensionsMapperOptions with { ForceSingle = true };

            public TopN_() : base("topN")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimension(Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimension>> factory)
                => this.AddOrUpdateSectionWithSectionFactory(nameof(Dimension), factory, mapperOptions);

            public TSelf Threshold(int threshold)
                => Self.AddOrUpdateSection(nameof(threshold), threshold);

            public TSelf Metric(Func<QueryElementFactory<TMetricArguments>.MetricSpec, IMetric> factory)
                => Self.AddOrUpdateSection(nameof(Metric), columnNames => factory(new(columnNames)));
        }

        public abstract class TopN<TDimension> : TopN_<TDimension, TDimension>
        {
        }

        public abstract class TopN<TDimension, TAggregations> :
            TopN_<TDimension, Dimension_Aggregations<TDimension, TAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class TopN<TDimension, TAggregations, TPostAggregations> :
            TopN_<TDimension, Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }

        public abstract class GroupBy_<TDimensions, TOrderByAndHavingArguments> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.GroupBy, TSelf>,
            IQuery<TArguments, TSelf, Marker.Dimensions>
        {
            public GroupBy_() : base("groupBy")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimensions(Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimensions>> factory)
                => this.AddOrUpdateSectionWithSectionFactory(nameof(Dimensions), factory, dimensionsMapperOptions);

            public TSelf LimitSpec(
                int? limit = null,
                int? offset = null,
                Func<QueryElementFactory<TOrderByAndHavingArguments>.OrderByColumnSpec, IEnumerable<ILimitSpec.OrderBy>>? columns = null)
                => Self.AddOrUpdateSection(nameof(LimitSpec), columnNames => new LimitSpec(limit, offset, columns?.Invoke(new(columnNames))));

            public TSelf Having(Func<QueryElementFactory<TOrderByAndHavingArguments>.Having, IHaving> factory)
                => Self.AddOrUpdateSection(nameof(Having), columnNames => factory(new(columnNames)));

            public TSelf HavingFilter(Func<QueryElementFactory<TOrderByAndHavingArguments>.Filter, IFilter> factory)
                => Self.AddOrUpdateSection(nameof(Having), columnNames => new QueryElementFactory<TOrderByAndHavingArguments>.Having(columnNames).Filter(factory));
        }

        public abstract class GroupBy<TDimensions> : GroupBy_<TDimensions, TDimensions>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations<TDimensions, TAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations, TPostAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }
    }
}
