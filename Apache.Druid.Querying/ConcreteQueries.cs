using Apache.Druid.Querying.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace Apache.Druid.Querying
{
    public readonly struct None
    {
    }

    public readonly record struct WithTimestamp<TResult>(DateTimeOffset Timestamp, TResult Result);
    public sealed record Source_VirtualColumns<TSource, TVirtualColumns>(TSource Source, TVirtualColumns VirtualColumns);
    public sealed record Dimension_Aggregations<TDimension, TAggregations>(TDimension Dimension, TAggregations Aggregations);
    public sealed record Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>(
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
    }

    internal static class IQueryWithMappedResult
    {
        public interface WithTimestamp_<TResult> : IQueryWithMappedResult<WithTimestamp<TResult>>
        {
            TResult MapResult(JsonElement from, JsonSerializerOptions options);

            WithTimestamp<TResult> IQueryWithMappedResult<WithTimestamp<TResult>>.Map(JsonElement json, JsonSerializerOptions options)
            {
                var t = json
                    .GetProperty(nameof(WithTimestamp<TResult>.Timestamp).ToCamelCase())
                    .Deserialize<DateTimeOffset>(options);
                var resultJson = json.GetProperty(nameof(WithTimestamp<TResult>.Timestamp).ToCamelCase());
                var result = MapResult(resultJson, options);
                return new(t, result);
            }
        }

        public interface Dimensions_Aggregations_<TDimensions, TAggregations>
            : WithTimestamp_<Dimensions_Aggregations<TDimensions, TAggregations>>
        {
            Dimensions_Aggregations<TDimensions, TAggregations> WithTimestamp_<Dimensions_Aggregations<TDimensions, TAggregations>>.MapResult(
                JsonElement json, JsonSerializerOptions options)
                => new(
                    json.Deserialize<TDimensions>(options)!,
                    json.Deserialize<TAggregations>(options)!);
        }

        public interface Aggregations_PostAggregations_<TAggregations, TPostAggregations>
            : WithTimestamp_<Aggregations_PostAggregations<TAggregations, TPostAggregations>>
        {
            Aggregations_PostAggregations<TAggregations, TPostAggregations> WithTimestamp_<Aggregations_PostAggregations<TAggregations, TPostAggregations>>.MapResult(
                JsonElement json, JsonSerializerOptions options) =>
                new(
                    json.Deserialize<TAggregations>(options)!,
                    json.Deserialize<TPostAggregations>(options)!);
        }

        public interface Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
             : WithTimestamp_<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
        {
            Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations> WithTimestamp_<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>.MapResult(
                JsonElement json, JsonSerializerOptions options) => new(
                json.Deserialize<TDimensions>(options)!,
                json.Deserialize<TAggregations>(options)!,
                json.Deserialize<TPostAggregations>(options)!);
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
                    IQueryWithMappedResult.Dimensions_Aggregations_<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<Source_VirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult.Dimensions_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
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
                    IQueryWithMappedResult.Dimensions_Aggregations_<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWithMappedResult.Dimensions_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }
    }
}
