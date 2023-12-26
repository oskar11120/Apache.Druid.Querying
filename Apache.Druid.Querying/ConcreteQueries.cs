using Apache.Druid.Querying.Internal;
using System;

namespace Apache.Druid.Querying
{
    public readonly struct None
    {
    }

    public readonly record struct Source_VirtualColumns<TSource, TVirtualColumns>(TSource Source, TVirtualColumns VirtualColumns);
    public readonly record struct WithTimestamp<TResult>(DateTimeOffset Timestamp, TResult Result);
    public readonly record struct Dimension_Aggregations<TDimension, TAggregations>(TDimension Dimension, TAggregations Aggregations);
    public readonly record struct Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>(
        TDimension Dimension, TAggregations Aggregations, TPostAggregations PostAggregations);
    public readonly record struct Aggregations_PostAggregations<TAggregations, TPostAggregations>(TAggregations Aggregations, TPostAggregations PostAggregations);
    public readonly record struct Dimensions_Aggregations<TDimensions, TAggregations>(TDimensions Dimensions, TAggregations Aggregations);
    public readonly record struct Dimensions_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>(
        TDimension Dimensions, TAggregations Aggregations, TPostAggregations PostAggregations);

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
        }
    }

    public static class Query<TSource>
    {
        public class TimeSeries :
            QueryBase<TSource, TimeSeries>.TimeSeries,
            IQueryWithResult<WithTimestamp<None>>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TimeSeries,
                IQueryWith.VirtualColumns<TVirtualColumns, WithVirtualColumns<TVirtualColumns>>,
                IQueryWithResult<WithTimestamp<None>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TimeSeries<TAggregations>,
                    IQueryWith.VirtualColumns<TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWithResult<WithTimestamp<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TimeSeries<TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult.Aggregations_PostAggregations_<TAggregations, TPostAggregations>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns :
                QueryBase<TSource, WithNoVirtualColumns>.TimeSeries,
                IQueryWithResult<WithTimestamp<None>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>>.TimeSeries<TAggregations>,
                    IQueryWithResult<WithTimestamp<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.TimeSeries<TAggregations, TPostAggregations>,
                        IQueryWithMappedResult.Aggregations_PostAggregations_<TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class TopN<TDimension> :
            QueryBase<TSource, TopN<TDimension>>.TopN<TDimension>,
            IQueryWithResult<WithTimestamp<TDimension>>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TopN<TDimension>,
                IQueryWith.VirtualColumns<TVirtualColumns, WithVirtualColumns<TVirtualColumns>>,
                IQueryWithResult<WithTimestamp<TDimension>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TopN<TDimension, TAggregations>,
                    IQueryWith.VirtualColumns<TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWithMappedResult.Dimension_Aggregations_<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns :
                QueryBase<TSource, WithNoVirtualColumns>.TopN<TDimension>,
                IQueryWithResult<WithTimestamp<TDimension>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>>.TopN<TDimension, TAggregations>,
                    IQueryWithMappedResult.Dimension_Aggregations_<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWithMappedResult.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class GroupBy<TDimensions> :
            QueryBase<TSource, GroupBy<TDimensions>>.GroupBy<TDimensions>,
            IQueryWithResult<WithTimestamp<TDimensions>>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.GroupBy<TDimensions>,
                IQueryWith.VirtualColumns<TVirtualColumns, WithVirtualColumns<TVirtualColumns>>,
                IQueryWithResult<WithTimestamp<TDimensions>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.GroupBy<TDimensions, TAggregations>,
                    IQueryWith.VirtualColumns<TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWithMappedResult.Dimensions_Aggregations_<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult.Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns :
                QueryBase<TSource, WithNoVirtualColumns>.GroupBy<TDimensions>,
                IQueryWithResult<WithTimestamp<TDimensions>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>>.GroupBy<TDimensions, TAggregations>,
                    IQueryWithMappedResult.Dimensions_Aggregations_<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWithMappedResult.Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }
    }
}
