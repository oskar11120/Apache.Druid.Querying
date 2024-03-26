using Apache.Druid.Querying.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Apache.Druid.Querying
{
    public readonly struct None
    {
    }

    public readonly record struct Source_VirtualColumns<TSource, TVirtualColumns>(TSource Source, TVirtualColumns VirtualColumns);
    public readonly record struct ScanResult<TValue>(string? SegmentId, TValue Value);

    public readonly record struct WithTimestamp<TValue>(DateTimeOffset Timestamp, TValue Value)
    {
        private static readonly byte[] timeColumnUtf8Bytes = Encoding.UTF8.GetBytes("__time");

        internal static readonly QueryResultElement.Deserializer<WithTimestamp<TValue>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
                => new(
                    context.DeserializeProperty<DateTimeOffset>(timeColumnUtf8Bytes),
                    context.Deserialize<TValue>());
    }

    public interface IQueryDataWithDimensions<TDimensions>
    {
        TDimensions Dimensions { get; }
    }

    public readonly record struct Dimension_Aggregations<TDimension, TAggregations>(TDimension Dimension, TAggregations Aggregations)
        : IQueryDataWithDimensions<TDimension>
    {
        TDimension IQueryDataWithDimensions<TDimension>.Dimensions => Dimension;

        internal static readonly QueryResultElement.Deserializer<Dimension_Aggregations<TDimension, TAggregations>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TDimension>(),
                    context.Deserialize<TAggregations>());
    }

    public readonly record struct Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>(
        TDimension Dimension, TAggregations Aggregations, TPostAggregations PostAggregations)
         : IQueryDataWithDimensions<TDimension>
    {
        TDimension IQueryDataWithDimensions<TDimension>.Dimensions => Dimension;

        internal static readonly QueryResultElement.Deserializer<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimension>(),
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
    }

    public readonly record struct Aggregations_PostAggregations<TAggregations, TPostAggregations>(TAggregations Aggregations, TPostAggregations PostAggregations)
    {
        internal static readonly QueryResultElement.Deserializer<Aggregations_PostAggregations<TAggregations, TPostAggregations>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
    }

    public readonly record struct Dimensions_Aggregations<TDimensions, TAggregations>(TDimensions Dimensions, TAggregations Aggregations)
        : IQueryDataWithDimensions<TDimensions>
    {
        internal static readonly QueryResultElement.Deserializer<Dimensions_Aggregations<TDimensions, TAggregations>> Deserializer =
            (ref QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimensions>(),
                    context.Deserialize<TAggregations>());
    }

    public readonly record struct Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>(
        TDimensions Dimensions, TAggregations Aggregations, TPostAggregations PostAggregations)
         : IQueryDataWithDimensions<TDimensions>
    {
        internal static readonly QueryResultElement.Deserializer<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>> Deserializer =
          (ref QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimensions>(),
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
    }

    public readonly record struct DataSourceMetadata(DateTimeOffset MaxIngestedEventTime);

    public static class QueryContext
    {
        public class TimeSeries : Context.WithVectorization
        {
            public bool? SkipEmptyBuckets { get; set; }
        }

        public class TopN : Context
        {
            public int? MinTopNThreshold { get; set; }
        }

        public class GroupBy : Context.WithVectorization
        {
            public long? MaxOnDiskStorage { get; set; }
            public bool? GroupByIsSingleThreaded { get; set; }
            public bool? BufferGrouperInitialBuckets { get; set; }
            public double? BufferGrouperMaxLoadFactor { get; set; }
            public bool? ForceHashAggregation { get; set; }
            public int? IntermediateCombineDegree { get; set; }
            public int? NumParallelCombineThreads { get; set; }
            public bool? MergeThreadLocal { get; set; }
            public bool? SortByDimsFirst { get; set; }
            public bool? ForceLimitPushDown { get; set; }
            public bool? ApplyLimitPushDownToSegment { get; set; }
            public bool? GroupByEnableMultiValueUnnesting { get; set; }
        }

        public class Scan : Context
        {
            public int? MaxRowsQueuedForOrdering { get; set; }
            public int? MaxSegmentPartitionsOrderedInMemory { get; set; }
        }
    }

    public static class Query<TSource>
    {
        public class TimeSeries : QueryBase<TSource, TSource, TimeSeries>.TimeSeries
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TimeSeries,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TimeSeries<TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            Source_VirtualColumns<TSource, TVirtualColumns>,
                            WithPostAggregations<TPostAggregations>>
                        .TimeSeries<TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, TSource, WithNoVirtualColumns>.TimeSeries
            {
                public class WithAggregations<TAggregations> : QueryBase<TSource, TSource, WithAggregations<TAggregations>>.TimeSeries<TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            TSource,
                            WithPostAggregations<TPostAggregations>>
                        .TimeSeries<TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class TopN<TDimension> : QueryBase<TSource, TSource, TopN<TDimension>>.TopN<TDimension>
            where TDimension : IEquatable<TDimension>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TopN<TDimension>,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<
                        TSource,
                        Source_VirtualColumns<TSource, TVirtualColumns>,
                        WithAggregations<TAggregations>>
                    .TopN<TDimension, TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            Source_VirtualColumns<TSource, TVirtualColumns>,
                            WithPostAggregations<TPostAggregations>>
                        .TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, TSource, WithNoVirtualColumns>.TopN<TDimension>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<
                        TSource,
                        TSource,
                        WithAggregations<TAggregations>>
                    .TopN<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            TSource,
                            WithPostAggregations<TPostAggregations>>
                        .TopN<TDimension, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class GroupBy<TDimensions> : QueryBase<TSource, TSource, GroupBy<TDimensions>>.GroupBy<TDimensions>
             where TDimensions : IEquatable<TDimensions>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.GroupBy<TDimensions>,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.GroupBy<TDimensions, TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            Source_VirtualColumns<TSource, TVirtualColumns>,
                            WithPostAggregations<TPostAggregations>>
                        .GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, TSource, WithNoVirtualColumns>.GroupBy<TDimensions>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, TSource, WithAggregations<TAggregations>>.GroupBy<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            TSource,
                            WithPostAggregations<TPostAggregations>>
                        .GroupBy<TDimensions, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class Scan : QueryBase<TSource, TSource, Scan>.Scan<TSource>
        {
        }

        public class SegmentMetadata :
            QueryBase,
            IQuery<SegmentMetadata>,
            IQueryWith.Intervals,
            IQueryWith.Context<Context, SegmentMetadata>,
            IQueryWithSource<TSource>.AndResult<Querying.SegmentMetadata>.AndDeserializationAndTruncatedResultHandling<None>,
            QueryResultDeserializer.Array<Querying.SegmentMetadata>
        {
            private static readonly IReadOnlyDictionary<Querying.SegmentMetadata.AnalysisType, string> analysisTypeStrings = Enum
                .GetValues<Querying.SegmentMetadata.AnalysisType>()
                .ToDictionary(type => type, type => type.ToString().ToCamelCase());
            private IQuery<SegmentMetadata> @base => this;

            public SegmentMetadata Merge(bool merge) => @base.AddOrUpdateSection(nameof(Merge), merge);
            public SegmentMetadata AnalysisTypes(IEnumerable<Querying.SegmentMetadata.AnalysisType> types)
                => @base.AddOrUpdateSection(nameof(AnalysisTypes), types.Select(type => analysisTypeStrings[type]));
            public SegmentMetadata AnalysisTypes(params Querying.SegmentMetadata.AnalysisType[] types)
                => AnalysisTypes(types.AsEnumerable());
            public SegmentMetadata AggregatorMergeStrategy(Querying.SegmentMetadata.AggregatorMergeStrategy strategy)
                => @base.AddOrUpdateSection(nameof(AggregatorMergeStrategy), strategy);

            IAsyncEnumerable<Querying.SegmentMetadata> IQueryWithSource<TSource>
                .AndResult<Querying.SegmentMetadata>.AndDeserializationAndTruncatedResultHandling<None>.OnTruncatedResultsSetQueryForRemaining(
                IAsyncEnumerable<Querying.SegmentMetadata> results, None context, Mutable<IQueryWithSource<TSource>> setter, CancellationToken token)
            {
                // TODO
                return results;
            }
        }

        public class DataSourceMetadata :
            QueryBase,
            IQueryWith.Context<Context, DataSourceMetadata>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestamp<Querying.DataSourceMetadata>,
            TruncatedQueryResultHandler<TSource>.TimeSeries<Querying.DataSourceMetadata>
        {
        }
    }
}
