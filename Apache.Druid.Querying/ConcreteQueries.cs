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

    internal static class MapResult
    {
        public static MapQueryResult<WithTimestamp<TResult>> WithTimestamp<TResult>(MapQueryResult<TResult> map)
            => Querying.WithTimestamp<TResult>.Map(map);
    }

    public readonly record struct WithTimestamp<TResult>(DateTimeOffset Timestamp, TResult Result)
    {
        private static readonly Dictionary<string, string> camelCaseNames = typeof(WithTimestamp<TResult>)
            .GetProperties()
            .ToDictionary(property => property.Name, property => property.Name.ToCamelCase());

        internal static MapQueryResult<WithTimestamp<TResult>> Map(MapQueryResult<TResult> map) => (json, options) =>
        {
            var t = json
                .GetProperty(camelCaseNames[nameof(WithTimestamp<TResult>.Timestamp)])
                .Deserialize<DateTimeOffset>(options);
            var resultJson = json.GetProperty(camelCaseNames[nameof(WithTimestamp<TResult>.Result)]);
            var result = map(resultJson, options);
            return new(t, result);
        };
    }

    public readonly record struct AggregationsAndPostAggregations<TAggregations, TPostAggregations>(TAggregations Aggregations, TPostAggregations PostAggregations)
    {
        internal static MapQueryResult<AggregationsAndPostAggregations<TAggregations, TPostAggregations>> Map { get; } = (json, options) => new(
            json.Deserialize<TAggregations>(options)!,
            json.Deserialize<TPostAggregations>(options)!);
    }

    public readonly record struct DimensionsAndAggregations<TDimensions, TAggregations>(TDimensions Dimensions, TAggregations Aggregations)
    {
        internal static MapQueryResult<DimensionsAndAggregations<TDimensions, TAggregations>> Map { get; } = (json, options) => new(
            json.Deserialize<TDimensions>(options)!,
            json.Deserialize<TAggregations>(options)!);
    }

    public readonly record struct DimensionsAggregationsAndPostAggregations<TDimension, TAggregations, TPostAggregations>(
        TDimension Dimensions, TAggregations Aggregations, TPostAggregations PostAggregations)
    {
        internal static MapQueryResult<DimensionsAggregationsAndPostAggregations<TDimension, TAggregations, TPostAggregations>> Map { get; } = (json, options) => new(
            json.Deserialize<TDimension>(options)!,
            json.Deserialize<TAggregations>(options)!,
            json.Deserialize<TPostAggregations>(options)!);
    }

    public static class QueryContext
    {
        public class TimeSeries : Context.WithVectorization
        {
            public bool? SkipEmptyBuckets { get; set; }
        }

        public class TopN : Context
        {
            public bool? MinTopNThreshold { get; set; }
        }
    }

    public static class Query<TSource>
    {
        public class TimeSeries :
            QueryBase<TSource, TimeSeries>.TimeSeries,
            IQueryWithResult<WithTimestamp<None>>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TimeSeries,
                IQueryWith.VirtualColumns<TVirtualColumns, WithVirtualColumns<TVirtualColumns>>,
                IQueryWithResult<WithTimestamp<None>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TimeSeries<TAggregations>,
                    IQueryWith.VirtualColumns<TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWithResult<WithTimestamp<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TimeSeries<TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>>
                    {
                        MapQueryResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>> IQueryWithMappedResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>>.Map
                        { get; } = MapResult.WithTimestamp(AggregationsAndPostAggregations<TAggregations, TPostAggregations>.Map);
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
                        IQueryWithMappedResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>>
                    {
                        MapQueryResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>> IQueryWithMappedResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>>.Map
                        { get; } = MapResult.WithTimestamp(AggregationsAndPostAggregations<TAggregations, TPostAggregations>.Map);
                    }
                }
            }
        }

        public class TopN<TDimension> :
            QueryBase<TSource, TopN<TDimension>>.TopN<TDimension>,
            IQueryWithResult<WithTimestamp<TDimension>>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TopN<TDimension>,
                IQueryWith.VirtualColumns<TVirtualColumns, WithVirtualColumns<TVirtualColumns>>,
                IQueryWithResult<WithTimestamp<TDimension>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TopN<TDimension, TAggregations>,
                    IQueryWith.VirtualColumns<TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWithMappedResult<WithTimestamp<DimensionsAndAggregations<TDimension, TAggregations>>>
                {
                    MapQueryResult<WithTimestamp<DimensionsAndAggregations<TDimension, TAggregations>>> IQueryWithMappedResult<WithTimestamp<DimensionsAndAggregations<TDimension, TAggregations>>>.Map
                        => MapResult.WithTimestamp(DimensionsAndAggregations<TDimension, TAggregations>.Map);

                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>>
                    {
                        MapQueryResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>> IQueryWithMappedResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>>.Map
                        { get; } = MapResult.WithTimestamp(AggregationsAndPostAggregations<TAggregations, TPostAggregations>.Map);
                    }
                }
            }

            public class WithNoVirtualColumns :
                QueryBase<TSource, WithNoVirtualColumns>.TopN<TDimension>,
                IQueryWithResult<WithTimestamp<TDimension>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>>.TopN<TDimension, TAggregations>,
                    IQueryWithMappedResult<WithTimestamp<DimensionsAndAggregations<TDimension, TAggregations>>>
                {
                    MapQueryResult<WithTimestamp<DimensionsAndAggregations<TDimension, TAggregations>>> IQueryWithMappedResult<WithTimestamp<DimensionsAndAggregations<TDimension, TAggregations>>>.Map
                        => MapResult.WithTimestamp(DimensionsAndAggregations<TDimension, TAggregations>.Map);

                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWithMappedResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>>
                    {
                        MapQueryResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>> IQueryWithMappedResult<WithTimestamp<AggregationsAndPostAggregations<TAggregations, TPostAggregations>>>.Map
                        { get; } = MapResult.WithTimestamp(AggregationsAndPostAggregations<TAggregations, TPostAggregations>.Map);
                    }
                }
            }
        }
    }
}
