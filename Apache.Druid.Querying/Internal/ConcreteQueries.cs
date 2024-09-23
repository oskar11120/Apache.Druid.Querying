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
            IQueryWith.Result<TElement>
            where TElementMapper : IQueryResultDeserializer<TElement>, new()
        {
            private static readonly IQueryResultDeserializer<TElement> array = new Array<TElement, TElementMapper>();

            IAsyncEnumerable<TElement> IQueryWith.Result<TElement>.Deserialize(
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

    public sealed record QueryTypeInternalState(string Type);
    public abstract class QueryBase :
        IQueryWithInternal.StateMappedToSection<QueryTypeInternalState, string>,
        IQueryWith.Intervals,
        IQueryWithInternal.MutableSectionAtomicity,
        IQueryWithInternal.PropertyColumnNameMappingChanges,
        IQueryWith.OnMapToJson
    {
        string IQueryWithInternal.Section<QueryTypeInternalState>.Key => "queryType";
        string IQueryWithInternal.StateMappedToSection<QueryTypeInternalState, string>.ToSection(QueryTypeInternalState state)
            => state.Type;
        public QueryBase(string? type = null)
            => (this as IQueryWithInternal.Section<QueryTypeInternalState>)
            .State = new(type ?? GetType().Name.ToCamelCase());

        SectionAtomicity.ImmutableBuilder? IQueryWithInternal.State<SectionAtomicity.ImmutableBuilder>.State { get; set; }
        ImmutableDictionary<Type, ApplyPropertyColumnNameMappingChanges>? IQueryWithInternal.State<ImmutableDictionary<Type, ApplyPropertyColumnNameMappingChanges>>.State { get; set; }
        List<OnMapQueryToJson>? IQueryWithInternal.State<List<OnMapQueryToJson>>.State { get; set; }
        QueryTypeInternalState? IQueryWithInternal.State<QueryTypeInternalState>.State { get; set; }
        IReadOnlyCollection<Interval>? IQueryWithInternal.State<IReadOnlyCollection<Interval>>.State { get; set; }
    }

    public static class QueryBase<TSource, TArguments, TSelf> where TSelf : IQueryWith.Self<TSelf>
    {
        public abstract class TimeSeries_<TResult> :
            QueryBase,
            IQueryWith.DescendingFlag,
            IQueryWith.Granularity,
            IQueryWith.Limit,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TimeSeries, TSelf>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestamp<TResult>,
            TruncatedQueryResultHandler<TSource>.TimeSeries<TResult>
        {
            public TimeSeries_() : base("timeseries")
            {
            }

            IQueryWith.DescendingFlag.InternalState? IQueryWithInternal.State<IQueryWith.DescendingFlag.InternalState>.State { get; set; }
            Granularity? IQueryWithInternal.State<Granularity>.State { get; set; }
            IQueryWithInternal.CreateSection<IFilter>? IQueryWithInternal.State<IQueryWithInternal.CreateSection<IFilter>>.State { get; set; }
            QueryContext.TimeSeries? IQueryWithInternal.State<QueryContext.TimeSeries>.State { get; set; }
            IQueryWith.Limit.InternalState? IQueryWithInternal.State<IQueryWith.Limit.InternalState>.State { get; set; }
        }

        public abstract class TimeSeries : TimeSeries_<None>
        {
        }

        public abstract class TimeSeries<TAggregations> :
            TimeSeries_<TAggregations>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }

        public abstract class TimeSeries<TAggregations, TPostAggregations> :
            TimeSeries_<Aggregations_PostAggregations<TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.PostAggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.PostAggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }

        public abstract class TopN_<TDimension, TMetricArgumentsAndResult> :
            QueryBase,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Dimesion<TArguments, TDimension, TSelf>,
            IQueryWith.Metric<TMetricArgumentsAndResult, TSelf>,
            IQueryWith.Threshold,
            IQueryWith.Context<QueryContext.TopN, TSelf>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestampAndArray<TMetricArgumentsAndResult>
        {
            public TopN_() : base("topN")
            {
            }

            Granularity? IQueryWithInternal.State<Granularity>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.CreateSection<IFilter>? IQueryWithInternal.State<IQueryWithInternal.CreateSection<IFilter>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Dimension>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Dimension>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.CreateSection<IMetric>? IQueryWithInternal.State<IQueryWithInternal.CreateSection<IMetric>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWith.Threshold.InternalState? IQueryWithInternal.State<IQueryWith.Threshold.InternalState>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            QueryContext.TopN? IQueryWithInternal.State<QueryContext.TopN>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }

        public abstract class TopN<TDimension> :
            TopN_<TDimension, TDimension>,
            DimensionsProvider<TDimension>.Identity,
            TruncatedQueryResultHandler<TSource>.TopN<TDimension, TDimension>
            where TDimension : IEquatable<TDimension>
        {
        }

        public abstract class TopN<TDimension, TAggregations> :
            TopN_<TDimension, Dimension_Aggregations<TDimension, TAggregations>>,
            DimensionsProvider<TDimension>.FromResult<Dimension_Aggregations<TDimension, TAggregations>>,
            TruncatedQueryResultHandler<TSource>.TopN<Dimension_Aggregations<TDimension, TAggregations>, TDimension>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
            where TDimension : IEquatable<TDimension>
        {
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }

        public abstract class TopN<TDimension, TAggregations, TPostAggregations> :
            TopN_<TDimension, Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>,
            DimensionsProvider<TDimension>.FromResult<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>,
            TruncatedQueryResultHandler<TSource>.TopN<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>, TDimension>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
            where TDimension : IEquatable<TDimension>
        {
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.PostAggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.PostAggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }

        public abstract class GroupBy_<TDimensions, TOrderByAndHavingArgumentsAndResult> :
            QueryBase,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Dimesions<TArguments, TDimensions, TSelf>,
            IQueryWith.LimitSpec<TOrderByAndHavingArgumentsAndResult, TSelf>,
            IQueryWith.Having<TOrderByAndHavingArgumentsAndResult, TSelf>,
            IQueryWith.Context<QueryContext.GroupBy, TSelf>,
            QueryResultDeserializer.ArrayOfGroupByResults<TOrderByAndHavingArgumentsAndResult>
        {
            public GroupBy_() : base("groupBy")
            {
            }

            Granularity? IQueryWithInternal.State<Granularity>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.CreateSection<IFilter>? IQueryWithInternal.State<IQueryWithInternal.CreateSection<IFilter>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Dimensions>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Dimensions>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWith.LimitSpec.InternalState? IQueryWithInternal.State<IQueryWith.LimitSpec.InternalState>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.CreateSection<IHaving>? IQueryWithInternal.State<IQueryWithInternal.CreateSection<IHaving>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            QueryContext.GroupBy? IQueryWithInternal.State<QueryContext.GroupBy>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }

        public abstract class GroupBy<TDimensions> : 
            GroupBy_<TDimensions, TDimensions>,
            DimensionsProvider<TDimensions>.Identity,
            TruncatedQueryResultHandler<TSource>.GroupBy<TDimensions, TDimensions>
            where TDimensions : IEquatable<TDimensions>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations<TDimensions, TAggregations>>,
            DimensionsProvider<TDimensions>.FromResult<Dimensions_Aggregations<TDimensions, TAggregations>>,
            TruncatedQueryResultHandler<TSource>.GroupBy<Dimensions_Aggregations<TDimensions, TAggregations>, TDimensions>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
            where TDimensions : IEquatable<TDimensions>
        {
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }

        public abstract class GroupBy<TDimensions, TAggregations, TPostAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>,
            DimensionsProvider<TDimensions>.FromResult<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>,
            TruncatedQueryResultHandler<TSource>.GroupBy<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>, TDimensions>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
            where TDimensions : IEquatable<TDimensions>
        {
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.Aggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.PostAggregations>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.PostAggregations>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }

        public abstract class Scan<TColumns> :
            QueryBase,
            IQueryWith.Order,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Limit,
            IQueryWith.BatchSize,
            IQueryWith.Context<QueryContext.Scan, TSelf>,
            QueryResultDeserializer.ArrayOfScanResults<TColumns>,
            TruncatedQueryResultHandler<TSource>.Scan<TColumns>
        {
            public Scan() : base("scan")
            {
            }

            OrderDirection? IQueryWithInternal.State<OrderDirection?>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWithInternal.CreateSection<IFilter>? IQueryWithInternal.State<IQueryWithInternal.CreateSection<IFilter>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWith.Limit.InternalState? IQueryWithInternal.State<IQueryWith.Limit.InternalState>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            IQueryWith.BatchSize.InternalState? IQueryWithInternal.State<IQueryWith.BatchSize.InternalState>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            QueryContext.Scan? IQueryWithInternal.State<QueryContext.Scan>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        }
    }

    public static class QueryBase<TSource>
    {
        public sealed record SegmentMetadataInternalState(bool Merge);
        public class SegmentMetadata :
            QueryBase,
            IQueryWith.Self<SegmentMetadata>,
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
