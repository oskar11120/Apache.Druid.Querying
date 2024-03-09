using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Apache.Druid.Querying.Internal
{
    public static partial class QueryResultDeserializer
    {
        public interface IArray<TElement, TElementMapper> :
            IQueryResultDeserializer<TElement>
            where TElementMapper : IQueryResultDeserializer<TElement>, new()
        {
            private static readonly IQueryResultDeserializer<TElement> array = new Array<TElement, TElementMapper>();

            IAsyncEnumerable<TElement> IQueryResultDeserializer<TElement>.Deserialize(
                QueryResultDeserializerContext context, CancellationToken token)
                => array.Deserialize(context, token);
        }

        public class WithTimestamp<TValue, TValueMapper>
            : TwoPropertyObject<DateTimeOffset, TValue, TValueMapper, WithTimestamp<TValue>>
            where TValueMapper : IQueryResultDeserializer<TValue>, new()
        {
            public WithTimestamp() : this("result")
            {
            }

            public WithTimestamp(string valuePropertyNameBase)
                : base(nameof(WithTimestamp<TValue>.Timestamp), valuePropertyNameBase, static (t, value) => new(t, value))
            {
            }
        }

        public sealed class GroupByResult<TValue> : WithTimestamp<TValue, Element<TValue>>
        {
            public GroupByResult() : base("event")
            {
            }
        }

        public sealed class ScanResult<TValue, TValueMapper>
            : TwoPropertyObject<string?, TValue, TValueMapper, ScanResult<TValue>>
            where TValueMapper : IQueryResultDeserializer<TValue>, new()
        {
            private static readonly string segmentId = nameof(ScanResult<TValue>.SegmentId).ToCamelCase();
            public ScanResult() : base(segmentId, "events", static (id, value) => new(id, value))
            {
            }
        }
        public interface ArrayOfObjectsWithTimestamp<TValue, TValueMapper> :
            IArray<WithTimestamp<TValue>, WithTimestamp<TValue, TValueMapper>>
            where TValueMapper : IQueryResultDeserializer<TValue>, new()
        {
        }

        public interface ArrayOfObjectsWithTimestamp<TValue> : ArrayOfObjectsWithTimestamp<TValue, Element<TValue>>
        {
        }

        public interface ArrayOfObjectsWithTimestampAndArray<TValue> : ArrayOfObjectsWithTimestamp<
            TValue,
            Array<TValue, Element<TValue>>>
        {
        }

        public interface ArrayOfGroupByResults<TValue> : IArray<WithTimestamp<TValue>, GroupByResult<TValue>>
        {
        }

        public interface ArrayOfScanResults<TColumns> :
            IArray<
                ScanResult<TColumns>,
                ScanResult<
                    TColumns,
                    Array<TColumns, Element<TColumns>>>>
        {
        }
    }

    public abstract class QueryBase : IQuery, IQueryWithSectionFactoryExpressions, IQueryWith.Intervals
    {
        public QueryBase(string? type = null)
        {
            state = new() { ["queryType"] = (_, _) => (type ?? GetType().Name.ToCamelCase())! };
        }

        private readonly Dictionary<string, QuerySectionValueFactory> state;
        Dictionary<string, QuerySectionValueFactory> IQuery.State => state;
        SectionAtomicity.IProvider.Builder IQueryWithSectionFactoryExpressions.SectionAtomicity { get; } = new();
        IReadOnlyCollection<Interval>? IQueryWith.Intervals.Intervals { get; set; }
    }

    public static class QueryBase<TSource, TArguments, TSelf> where TSelf : IQuery<TSelf>
    {
        public abstract class TimeSeries_<TResult> :
            QueryBase,
            IQueryWith.Order,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TimeSeries, TSelf>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestamp<TResult>,
            TruncatedQueryResultHandler<TSource>.TimeSeries<TResult>
        {
            public TimeSeries_() : base("timeseries")
            {
            }
        }

        public abstract class TimeSeries : TimeSeries_<None>
        {
        }

        public abstract class TimeSeries<TAggregations> :
            TimeSeries_<TAggregations>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class TimeSeries<TAggregations, TPostAggregations> :
            TimeSeries_<Aggregations_PostAggregations<TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }

        public abstract class TopN_<TDimension, TMetricArgumentsAndResult, TDimensionProvider> :
            QueryBase,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>,
            IQueryWith.Dimesion<TArguments, TDimension, TSelf>,
            IQueryWith.Threshold,
            IQueryWith.Metric<TMetricArgumentsAndResult, TSelf>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestampAndArray<TMetricArgumentsAndResult>,
            TruncatedQueryResultHandler<TSource>.TopN_GroupBy<TMetricArgumentsAndResult, TDimension, TDimensionProvider>
            where TDimension : IEquatable<TDimension>
            where TDimensionProvider : IDimensionsProvider<TMetricArgumentsAndResult, TDimension>, new()
        {
            public TopN_() : base("topN")
            {
            }
        }

        public abstract class TopN<TDimension> : TopN_<TDimension, TDimension, DimensionsProvider<TDimension>.Identity>
            where TDimension : IEquatable<TDimension>
        {
        }

        public abstract class TopN<TDimension, TAggregations> :
            TopN_<
                TDimension,
                Dimension_Aggregations<TDimension, TAggregations>,
                DimensionsProvider<TDimension>.FromResult<Dimension_Aggregations<TDimension, TAggregations>>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
            where TDimension : IEquatable<TDimension>
        {
        }

        public abstract class TopN<TDimension, TAggregations, TPostAggregations> :
            TopN_<
                TDimension,
                Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>,
                DimensionsProvider<TDimension>.FromResult<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
            where TDimension : IEquatable<TDimension>
        {
        }

        public abstract class GroupBy_<TDimensions, TOrderByAndHavingArgumentsAndResult, TDimensionProvider> :
            QueryBase,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.GroupBy, TSelf>,
            IQueryWith.Dimesions<TArguments, TDimensions, TSelf>,
            IQueryWith.LimitSpec<TOrderByAndHavingArgumentsAndResult, TSelf>,
            IQueryWith.Having<TOrderByAndHavingArgumentsAndResult, TSelf>,
            QueryResultDeserializer.ArrayOfGroupByResults<TOrderByAndHavingArgumentsAndResult>,
            TruncatedQueryResultHandler<TSource>.TopN_GroupBy<TOrderByAndHavingArgumentsAndResult, TDimensions, TDimensionProvider>
            where TDimensions : IEquatable<TDimensions>
            where TDimensionProvider : IDimensionsProvider<TOrderByAndHavingArgumentsAndResult, TDimensions>, new()
        {
            public GroupBy_() : base("groupBy")
            {
            }
        }

        public abstract class GroupBy<TDimensions> : GroupBy_<TDimensions, TDimensions, DimensionsProvider<TDimensions>.Identity>
            where TDimensions : IEquatable<TDimensions>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations> :
            GroupBy_<
                TDimensions,
                Dimensions_Aggregations<TDimensions, TAggregations>,
                DimensionsProvider<TDimensions>.FromResult<Dimensions_Aggregations<TDimensions, TAggregations>>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
            where TDimensions : IEquatable<TDimensions>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations, TPostAggregations> :
            GroupBy_<
                TDimensions,
                Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>,
                DimensionsProvider<TDimensions>.FromResult<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
            where TDimensions : IEquatable<TDimensions>
        {
        }

        public abstract class Scan<TColumns> :
            QueryBase,
            IQueryWith.Order,
            IQueryWith.OffsetAndLimit,
            IQueryWith.BatchSize,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.Scan, TSelf>,
            QueryResultDeserializer.ArrayOfScanResults<TColumns>,
            TruncatedQueryResultHandler<TSource>.Scan<ScanResult<TColumns>>
        {
            int IQueryWith.OffsetAndLimit.Offset { get; set; }
            int IQueryWith.OffsetAndLimit.Limit { get; set; }

            public Scan() : base("scan")
            {
            }
        }
    }
}
