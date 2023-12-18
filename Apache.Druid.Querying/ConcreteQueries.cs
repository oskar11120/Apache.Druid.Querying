

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

    public readonly record struct Pair<TAggregations, TPostAggregations>(TAggregations Aggregations, TPostAggregations PostAggregations)
    {
        internal static MapQueryResult<Pair<TAggregations, TPostAggregations>> Map { get; } = (json, options) => new(
            json.Deserialize<TAggregations>(options)!,
            json.Deserialize<TPostAggregations>(options)!);
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
                    QueryBase<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TimeSeries,
                    IQueryWith.VirtualColumns<TVirtualColumns, WithAggregations<TAggregations>>,
                    IQueryWith.Aggregations<SourceWithVirtualColumns<TSource, TVirtualColumns>, TAggregations, WithAggregations<TAggregations>>,
                    IQueryWithResult<WithTimestamp<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithPostAggregations<TPostAggregations>>.TimeSeries,
                        IQueryWith.VirtualColumns<TVirtualColumns, WithPostAggregations<TPostAggregations>>,
                        IQueryWith.Aggregations<SourceWithVirtualColumns<TSource, TVirtualColumns>, TAggregations, WithPostAggregations<TPostAggregations>>,
                        IQueryWith.PostAggregations<TAggregations, TPostAggregations, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<WithTimestamp<Pair<TAggregations, TPostAggregations>>>
                    {
                        MapQueryResult<WithTimestamp<Pair<TAggregations, TPostAggregations>>> IQueryWithMappedResult<WithTimestamp<Pair<TAggregations, TPostAggregations>>>.Map
                        { get; } = WithTimestamp<Pair<TAggregations, TPostAggregations>>.Map(Pair<TAggregations, TPostAggregations>.Map);
                    }
                }
            }

            public class WithNoVirtualColumns :
                QueryBase<TSource, WithNoVirtualColumns>.TimeSeries,
                IQueryWithResult<WithTimestamp<None>>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, WithAggregations<TAggregations>>.TimeSeries,
                    IQueryWith.Aggregations<TSource, TAggregations, WithAggregations<TAggregations>>,
                    IQueryWithResult<WithTimestamp<TAggregations>>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<TSource, WithPostAggregations<TPostAggregations>>.TimeSeries,
                        IQueryWith.Aggregations<TSource, TAggregations, WithPostAggregations<TPostAggregations>>,
                        IQueryWith.PostAggregations<TAggregations, TPostAggregations, WithPostAggregations<TPostAggregations>>,
                        IQueryWithMappedResult<WithTimestamp<Pair<TAggregations, TPostAggregations>>>
                    {
                        MapQueryResult<WithTimestamp<Pair<TAggregations, TPostAggregations>>> IQueryWithMappedResult<WithTimestamp<Pair<TAggregations, TPostAggregations>>>.Map
                        { get; } = WithTimestamp<Pair<TAggregations, TPostAggregations>>.Map(Pair<TAggregations, TPostAggregations>.Map);
                    }
                }
            }
        }
    }
}
