using Apache.Druid.Querying.Internal;
using System;
using System.Text;

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
            private static readonly byte[] timeColumnUtf8Bytes = Encoding.UTF8.GetBytes("__time");

            public WithTimestamp<TValue> Deserialize(ref QueryResultElement.DeserializerContext context)
                => new(
                    context.DeserializeProperty<DateTimeOffset>(timeColumnUtf8Bytes),
                    context.Deserialize<TValue>());
        }
    }

    public readonly record struct Dimension_Aggregations<TDimension, TAggregations>(TDimension Dimension, TAggregations Aggregations) 
        : IWithDimensions<TDimension>
    {
        TDimension IWithDimensions<TDimension>.Dimensions => Dimension;

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
         : IWithDimensions<TDimension>
    {
        TDimension IWithDimensions<TDimension>.Dimensions => Dimension;

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
        : IWithDimensions<TDimensions>
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
         : IWithDimensions<TDimensions>
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
        public class TimeSeries : QueryBase<TSource, TimeSeries, None>.TimeSeries
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>, None>.TimeSeries,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>, TAggregations>.TimeSeries<TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            Source_VirtualColumns<TSource, TVirtualColumns>, 
                            WithPostAggregations<TPostAggregations>, 
                            Aggregations_PostAggregations<TAggregations, TPostAggregations>>
                        .TimeSeries<TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, WithNoVirtualColumns, None>.TimeSeries
            {
                public class WithAggregations<TAggregations> : QueryBase<TSource, WithAggregations<TAggregations>, TAggregations>.TimeSeries<TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> : 
                        QueryBase<
                            TSource, 
                            WithPostAggregations<TPostAggregations>, 
                            Aggregations_PostAggregations<TAggregations, TPostAggregations>>
                        .TimeSeries<TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class TopN<TDimension> : QueryBase<TSource, TopN<TDimension>, TDimension>.TopN<TDimension>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>, TDimension>.TopN<TDimension>,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<
                        Source_VirtualColumns<TSource, TVirtualColumns>, 
                        WithAggregations<TAggregations>, 
                        Dimensions_Aggregations<TDimension, TAggregations>>
                    .TopN<TDimension, TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            Source_VirtualColumns<TSource, TVirtualColumns>, 
                            WithPostAggregations<TPostAggregations>, 
                            Dimensions_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>
                        .TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, WithNoVirtualColumns, TDimension>.TopN<TDimension>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<
                        TSource, 
                        WithAggregations<TAggregations>, 
                        Dimensions_Aggregations<TDimension, TAggregations>>
                    .TopN<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource, 
                            WithPostAggregations<TPostAggregations>, 
                            Dimensions_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>
                        .TopN<TDimension, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class GroupBy<TDimensions> : QueryBase<TSource, GroupBy<TDimensions>, TDimensions>.GroupBy<TDimensions>
        {
            public class WithVirtualColumns<TVirtualColumns> : 
                QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>, TDimensions>.GroupBy<TDimensions>,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>, Dimensions_Aggregations<TDimensions, TAggregations>>.GroupBy<TDimensions, TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            Source_VirtualColumns<TSource, TVirtualColumns>, 
                            WithPostAggregations<TPostAggregations>,
                            Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
                        .GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, WithNoVirtualColumns, TDimensions>.GroupBy<TDimensions>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>, Dimensions_Aggregations<TDimensions, TAggregations>>.GroupBy<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource, 
                            WithPostAggregations<TPostAggregations>, 
                            Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
                        .GroupBy<TDimensions, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class Scan : QueryBase<TSource, Scan, TSource>.Scan
        {
            public class WithColumns<TColumns> : QueryBase<TColumns, WithColumns<TColumns>, TColumns>.Scan.WithColumns
            {
            }
        }
    }
}
