using Apache.Druid.Querying.Internal;
using System;

namespace Apache.Druid.Querying
{
    public readonly struct None
    {
    }

    public readonly record struct Source_VirtualColumns<TSource, TVirtualColumns>(TSource Source, TVirtualColumns VirtualColumns);
    public readonly record struct ScanResult<TValue>(string? SegmentId, TValue Value);

    public readonly record struct WithTimestamp<TValue>(DateTimeOffset Timestamp, TValue Value)
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<WithTimestamp<TValue>>
        {
            public WithTimestamp<TValue> Deserialize(ref QueryResultElement.DeserializerContext context)
                => new(
                    context.DeserializeTimeProperty(),
                    context.Deserialize<TValue>());
        }
    }

    public readonly record struct Dimension_Aggregations<TDimension, TAggregations>(TDimension Dimension, TAggregations Aggregations) 
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<Dimension_Aggregations<TDimension, TAggregations>>
        {
            public Dimension_Aggregations<TDimension, TAggregations> Deserialize(ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TDimension>(),
                    context.Deserialize<TAggregations>());
        }
    }

    public readonly record struct Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>(
        TDimension Dimension, TAggregations Aggregations, TPostAggregations PostAggregations)
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>
        {
            public Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations> Deserialize(ref QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimension>(),
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        }
    }

    public readonly record struct Aggregations_PostAggregations<TAggregations, TPostAggregations>(TAggregations Aggregations, TPostAggregations PostAggregations)
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<Aggregations_PostAggregations<TAggregations, TPostAggregations>>
        {
            public Aggregations_PostAggregations<TAggregations, TPostAggregations> Deserialize(ref QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        }
    }

    public readonly record struct Dimensions_Aggregations<TDimensions, TAggregations>(TDimensions Dimensions, TAggregations Aggregations)
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<Dimensions_Aggregations<TDimensions, TAggregations>>
        {
            public Dimensions_Aggregations<TDimensions, TAggregations> Deserialize(ref QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimensions>(),
                    context.Deserialize<TAggregations>());
        }
    }

    public readonly record struct Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>(
        TDimensions Dimensions, TAggregations Aggregations, TPostAggregations PostAggregations)
    {
        private sealed class Deserializer : QueryResultElement.IDeserializer<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
        {
            public Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations> Deserialize(ref QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimensions>(),
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        }
    }

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
                        IQueryWithMappedResult<TSource>.Aggregations_PostAggregations_<TAggregations, TPostAggregations>
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
                        IQueryWithMappedResult<TSource>.Aggregations_PostAggregations_<TAggregations, TPostAggregations>
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
                    IQueryWithMappedResult<TSource>.Dimension_Aggregations_<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<TSource>.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
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
                    IQueryWithMappedResult<TSource>.Dimension_Aggregations_<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWithMappedResult<TSource>.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
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
                    IQueryWithMappedResult<TSource>.Dimensions_Aggregations_<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<TSource>.Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
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
                    IQueryWithMappedResult<TSource>.Dimensions_Aggregations_<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWithMappedResult<TSource>.Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class Scan :
           QueryBase<TSource, Scan>.Scan,
           IQueryWithMappedResult<TSource>.ScanResult_<TSource>
        {
            public class WithColumns<TColumns> :
                QueryBase<TColumns, WithColumns<TColumns>>.Scan.WithColumns,
                IQueryWithMappedResult<TSource>.ScanResult_<TColumns>
            {
            }
        }
    }
}
