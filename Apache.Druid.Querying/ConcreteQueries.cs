using Apache.Druid.Querying.Internal;
using System;

namespace Apache.Druid.Querying
{
    public readonly struct None
    {
    }

    public readonly record struct Source_VirtualColumns<TSource, TVirtualColumns>(TSource Source, TVirtualColumns VirtualColumns);
    public readonly record struct WithTimestamp<TValue>(DateTimeOffset Timestamp, TValue Value);
    public readonly record struct Dimension_Aggregations<TDimension, TAggregations>(TDimension Dimension, TAggregations Aggregations);
    public readonly record struct Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>(
        TDimension Dimension, TAggregations Aggregations, TPostAggregations PostAggregations);
    public readonly record struct Aggregations_PostAggregations<TAggregations, TPostAggregations>(TAggregations Aggregations, TPostAggregations PostAggregations);
    public readonly record struct Dimensions_Aggregations<TDimensions, TAggregations>(TDimensions Dimensions, TAggregations Aggregations);
    public readonly record struct Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>(
        TDimensions Dimensions, TAggregations Aggregations, TPostAggregations PostAggregations);
    public readonly record struct ScanResult<TValue>(string? SegmentId, TValue Value);

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
    }

    public static class Query<TSource>
    {
        public class TimeSeries :
            QueryBase<TSource, TimeSeries>.TimeSeries,
            IQueryWithSource<TSource>.AndResult<WithTimestamp<None>>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TimeSeries,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>,
                IQueryWithSource<TSource>.AndResult<WithTimestamp<None>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TimeSeries<TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWithSource<TSource>.AndResult<WithTimestamp<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TimeSeries<TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<TSource>.WithTimestamp.Aggregations_PostAggregations_<TAggregations, TPostAggregations>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns :
                QueryBase<TSource, WithNoVirtualColumns>.TimeSeries,
                IQueryWithSource<TSource>.AndResult<WithTimestamp<None>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>>.TimeSeries<TAggregations>,
                    IQueryWithSource<TSource>.AndResult<WithTimestamp<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.TimeSeries<TAggregations, TPostAggregations>,
                        IQueryWithMappedResult<TSource>.WithTimestamp.Aggregations_PostAggregations_<TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class TopN<TDimension> :
            QueryBase<TSource, TopN<TDimension>>.TopN<TDimension>,
            IQueryWithSource<TSource>.AndResult<WithTimestamp<TDimension>>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TopN<TDimension>,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>,
                IQueryWithSource<TSource>.AndResult<WithTimestamp<TDimension>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TopN<TDimension, TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWithMappedResult<TSource>.WithTimestamp.Dimension_Aggregations_<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<TSource>.WithTimestamp.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns :
                QueryBase<TSource, WithNoVirtualColumns>.TopN<TDimension>,
                IQueryWithSource<TSource>.AndResult<WithTimestamp<TDimension>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>>.TopN<TDimension, TAggregations>,
                    IQueryWithMappedResult<TSource>.WithTimestamp.Dimension_Aggregations_<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWithMappedResult<TSource>.WithTimestamp.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class GroupBy<TDimensions> :
            QueryBase<TSource, GroupBy<TDimensions>>.GroupBy<TDimensions>,
            IQueryWithSource<TSource>.AndResult<WithTimestamp<TDimensions>>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.GroupBy<TDimensions>,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>,
                IQueryWithSource<TSource>.AndResult<WithTimestamp<TDimensions>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.GroupBy<TDimensions, TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWithMappedResult<TSource>.WithTimestamp.Dimensions_Aggregations_<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<TSource>.WithTimestamp.Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns :
                QueryBase<TSource, WithNoVirtualColumns>.GroupBy<TDimensions>,
                IQueryWithSource<TSource>.AndResult<WithTimestamp<TDimensions>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>>.GroupBy<TDimensions, TAggregations>,
                    IQueryWithMappedResult<TSource>.WithTimestamp.Dimensions_Aggregations_<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWithMappedResult<TSource>.WithTimestamp.Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }
    }
}
