using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
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
                QueryResultDeserializationContext context, CancellationToken token)
                => array.Deserialize(context, token);
        }

        public interface Array<TElement> : IArray<TElement, Element<TElement>>
        {
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

    public sealed record QueryBaseInternalState(string Type);
    public abstract class QueryBase :
        IQueryWithInternal.Section<QueryBaseInternalState>,
        IQueryWith.Intervals,
        IQueryWithInternal.MutableSectionAtomicity,
        IQueryWithInternal.SectionFactoryExpressionStates,
        IQueryWithInternal.PropertyColumnNameMappingChanges
    {
        public QueryBase(string? type = null)
            => (this as IQueryWithInternal.Section<QueryBaseInternalState>)
            .SetState("queryType", new(type ?? GetType().Name.ToCamelCase()), state => state.Type);

        QuerySectionState<QueryBaseInternalState>? IQueryWithInternal.State<QuerySectionState<QueryBaseInternalState>>.State { get; set; }
        SectionAtomicity.ImmutableBuilder? IQueryWithInternal.State<SectionAtomicity.ImmutableBuilder>.State { get; set; }
        Dictionary<string, GetQuerySectionJson>? IQueryWithInternal.State<Dictionary<string, GetQuerySectionJson>>.State { get; set; }
        QuerySectionState<IReadOnlyCollection<Interval>>? IQueryWithInternal.State<QuerySectionState<IReadOnlyCollection<Interval>>>.State { get; set; }
        ImmutableDictionary<Type, ApplyPropertyColumnNameMappingChanges>? IQueryWithInternal.State<ImmutableDictionary<Type, ApplyPropertyColumnNameMappingChanges>>.State { get; set; }
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

            QuerySectionState<OrderDirection>? IQueryWithInternal.State<QuerySectionState<OrderDirection>>.State { get; set; }
            QuerySectionState<Granularity>? IQueryWithInternal.State<QuerySectionState<Granularity>>.State { get; set; }
            QuerySectionFactoryState<IFilter>? IQueryWithInternal.State<QuerySectionFactoryState<IFilter>>.State { get; set; }
            QuerySectionState<QueryContext.TimeSeries>? IQueryWithInternal.State<QuerySectionState<QueryContext.TimeSeries>>.State { get; set; }
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
            IQueryWith.Dimesion<TArguments, TDimension, TSelf>,
            IQueryWith.Metric<TMetricArgumentsAndResult, TSelf>,
            IQueryWith.Threshold,
            IQueryWith.Context<QueryContext.TopN, TSelf>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestampAndArray<TMetricArgumentsAndResult>,
            TruncatedQueryResultHandler<TSource>.TopN_GroupBy<TMetricArgumentsAndResult, TDimension, TDimensionProvider>
            where TDimension : IEquatable<TDimension>
            where TDimensionProvider : IDimensionsProvider<TMetricArgumentsAndResult, TDimension>, new()
        {
            public TopN_() : base("topN")
            {
            }

            QuerySectionState<Granularity>? IQueryWithInternal.State<QuerySectionState<Granularity>>.State { get; set; }
            QuerySectionFactoryState<IFilter>? IQueryWithInternal.State<QuerySectionFactoryState<IFilter>>.State { get; set; }
            QuerySectionState<QueryContext.TopN>? IQueryWithInternal.State<QuerySectionState<QueryContext.TopN>>.State { get; set; }
            QuerySectionState<IQueryWith.Threshold.InternalState>? IQueryWithInternal.State<QuerySectionState<IQueryWith.Threshold.InternalState>>.State { get; set; }
            QuerySectionFactoryState<IMetric>? IQueryWithInternal.State<QuerySectionFactoryState<IMetric>>.State { get; set; }
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
            IQueryWith.Dimesions<TArguments, TDimensions, TSelf>,
            IQueryWith.LimitSpec<TOrderByAndHavingArgumentsAndResult, TSelf>,
            IQueryWith.Having<TOrderByAndHavingArgumentsAndResult, TSelf>,
            IQueryWith.Context<QueryContext.GroupBy, TSelf>,
            QueryResultDeserializer.ArrayOfGroupByResults<TOrderByAndHavingArgumentsAndResult>,
            TruncatedQueryResultHandler<TSource>.TopN_GroupBy<TOrderByAndHavingArgumentsAndResult, TDimensions, TDimensionProvider>
            where TDimensions : IEquatable<TDimensions>
            where TDimensionProvider : IDimensionsProvider<TOrderByAndHavingArgumentsAndResult, TDimensions>, new()
        {
            public GroupBy_() : base("groupBy")
            {
            }

            QuerySectionState<Granularity>? IQueryWithInternal.State<QuerySectionState<Granularity>>.State { get; set; }
            QuerySectionFactoryState<IFilter>? IQueryWithInternal.State<QuerySectionFactoryState<IFilter>>.State { get; set; }
            QuerySectionState<QueryContext.GroupBy>? IQueryWithInternal.State<QuerySectionState<QueryContext.GroupBy>>.State { get; set; }
            QuerySectionFactoryState<ILimitSpec>? IQueryWithInternal.State<QuerySectionFactoryState<ILimitSpec>>.State { get; set; }
            QuerySectionFactoryState<IHaving>? IQueryWithInternal.State<QuerySectionFactoryState<IHaving>>.State { get; set; }
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
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Limit,
            IQueryWith.BatchSize,
            IQueryWith.Context<QueryContext.Scan, TSelf>,
            QueryResultDeserializer.ArrayOfScanResults<TColumns>,
            TruncatedQueryResultHandler<TSource>.Scan<ScanResult<TColumns>>
        {
            public Scan() : base("scan")
            {
            }

            QuerySectionState<OrderDirection>? IQueryWithInternal.State<QuerySectionState<OrderDirection>>.State { get; set; }
            QuerySectionState<IQueryWith.Limit.InternalState>? IQueryWithInternal.State<QuerySectionState<IQueryWith.Limit.InternalState>>.State { get; set; }
            QuerySectionState<IQueryWith.BatchSize.InternalState>? IQueryWithInternal.State<QuerySectionState<IQueryWith.BatchSize.InternalState>>.State { get; set; }
            QuerySectionFactoryState<IFilter>? IQueryWithInternal.State<QuerySectionFactoryState<IFilter>>.State { get; set; }
            QuerySectionState<QueryContext.Scan>? IQueryWithInternal.State<QuerySectionState<QueryContext.Scan>>.State { get; set; }
            QuerySectionState<IQueryWith.Offset.InternalState>? IQueryWithInternal.State<QuerySectionState<IQueryWith.Offset.InternalState>>.State { get; set; }
        }
    }

    public static class QueryBase<TSource>
    {
        public sealed record SegmentMetadataInternalState(bool Merge);
        public class SegmentMetadata :
            QueryBase,
            IQuery<SegmentMetadata>,
            IQueryWithInternal.Section<SegmentMetadataInternalState>,
            IQueryWithInternal.Section<IReadOnlyCollection<Querying.SegmentMetadata.AnalysisType>>,
            IQueryWithInternal.Section<Querying.SegmentMetadata.AggregatorMergeStrategy>,
            IQueryWith.Context<Context, SegmentMetadata>,
            QueryResultDeserializer.Array<Querying.SegmentMetadata>,
            TruncatedQueryResultHandler<TSource>.SegmentMetadata
        {
            private protected static readonly IReadOnlyDictionary<Querying.SegmentMetadata.AnalysisType, string> AnalysisTypeStrings = Enum
                .GetValues<Querying.SegmentMetadata.AnalysisType>()
                .ToDictionary(type => type, type => type.ToString().ToCamelCase());
            private protected IQueryWithInternal.Section<SegmentMetadataInternalState> MergeSection => this;
            private protected IQueryWithInternal.Section<IReadOnlyCollection<Querying.SegmentMetadata.AnalysisType>> AnalysisTypesSection => this;
            private protected IQueryWithInternal.Section<Querying.SegmentMetadata.AggregatorMergeStrategy> MergeStrategySection => this;

            QuerySectionState<Context>? IQueryWithInternal.State<QuerySectionState<Context>>.State { get; set; }
            QuerySectionState<QueryBase<TSource>.SegmentMetadataInternalState>? IQueryWithInternal.State<QuerySectionState<QueryBase<TSource>.SegmentMetadataInternalState>>.State { get; set; }
            QuerySectionState<IReadOnlyCollection<Querying.SegmentMetadata.AnalysisType>>? IQueryWithInternal.State<QuerySectionState<IReadOnlyCollection<Querying.SegmentMetadata.AnalysisType>>>.State { get; set; }
            QuerySectionState<Querying.SegmentMetadata.AggregatorMergeStrategy>? IQueryWithInternal.State<QuerySectionState<Querying.SegmentMetadata.AggregatorMergeStrategy>>.State { get; set; }
        }
    }
}
