using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Apache.Druid.Querying
{
    public interface IQuery
    {
        string QueryType { get; }

        private protected Dictionary<string, Action<JsonObject>> ComponentWrites { get; }
        internal void AddOrUpdateComponent<TComponent>(string key, TComponent component)
        {
            ComponentWrites.Remove(key);
            ComponentWrites.Add(key, json => json[key] = JsonSerializer.SerializeToNode(component));
        }
    }

    public interface IQuery<TSource, TSelf> : IQuery
    {
        public TSelf AsSelf => (TSelf)this;
    }

    public sealed record Interval(DateTimeOffset From, DateTimeOffset To);
    public enum Order
    {
        Ascending,
        Descending
    }
    public enum Granularity
    {
        None,
        Second,
        Minute,
        FiveMinutes,
        TenMinutes,
        FifteenMinutes,
        ThrityMinutes,
        Hour,
        SixHours,
        EightHours,
        Day,
        Week,
        Month,
        Quarter,
        Year,
        All
    }
    public sealed record SourceWithVirtualColumns<TSource, TVirtualColumns>(TSource Source, TVirtualColumns Columns);

    public static class IQueryWith
    {
        public interface VirtualColumns<TSource, TVirtualColumns, TQuery, TQueryWithVirtualColumns>
            : IQuery<TSource, TQuery>
            where TQueryWithVirtualColumns : IQuery<SourceWithVirtualColumns<TSource, TVirtualColumns>, TQueryWithVirtualColumns>
        {
            internal TQueryWithVirtualColumns AsTypeWithVirtualColumns { get; }
        }

        public interface Filter<TSource, TQuery> : IQuery<TSource, TQuery>
        {
        }

        public interface Aggregators<TSource, TAggregations, TSelf> : IQuery<TSource, TSelf>
        {
            internal static readonly HashSet<string> ResultPropertyNames = typeof(TAggregations)
                .GetProperties()
                .Select(property => property.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            internal interface AndPostAggregators<TPostAggregations> : Aggregators<TSource, TAggregations, TSelf>
            {
            }
        }

        public interface Intervals : IQuery
        {
        }

        public interface Order : IQuery
        {
        }

        public interface Granularity : IQuery
        {
        }
    }

    public static class QueryExtensions
    {
        public static TQueryWithVirtualColumns WithVirtualColumns<TSource, TVirtualColumns, TQuery, TQueryWithVirtualColumns>(
            this IQueryWith.VirtualColumns<TSource, TVirtualColumns, TQuery, TQueryWithVirtualColumns> query,
            Func<Factory<TSource>.VirtualColumns<TVirtualColumns>, IEnumerable<VirtualColumn>> factory)
            where TQueryWithVirtualColumns : IQuery<SourceWithVirtualColumns<TSource, TVirtualColumns>, TQueryWithVirtualColumns>
        {
            var factory_ = new Factory<TSource>.VirtualColumns<TVirtualColumns>();
            var virtualColumns = factory(factory_);
            query.AddOrUpdateComponent(nameof(virtualColumns), virtualColumns);
            return query.AsTypeWithVirtualColumns;
        }

        public static TQuery WithFilter<TSource, TQuery>(this IQuery<TSource, TQuery> query, Func<Factory<TSource>.Filter, Filter> factory)
            where TQuery : IQueryWith.Filter<TSource, TQuery>
        {
            var factory_ = new Factory<TSource>.Filter();
            var filter = factory(factory_);
            query.AddOrUpdateComponent(nameof(filter), filter);
            return query.AsSelf;
        }

        public static TQuery WithAggregators<TSource, TQuery, TAggregations>(
            this IQueryWith.Aggregators<TSource, TAggregations, TQuery> query, Func<Factory<TSource>.Aggregators<TAggregations>, IEnumerable<Aggregator>> factory)
            where TQuery : IQueryWith.Aggregators<TSource, TAggregations, TQuery>
        {
            var factory_ = new Factory<TSource>.Aggregators<TAggregations>();
            var aggregations = factory(factory_).ToArray();
            var aggregatorNames = aggregations.Select(aggregator => aggregator.Name);
            var resultPropertyNames = IQueryWith.Aggregators<TSource, TAggregations, TQuery>.ResultPropertyNames;
            var match = resultPropertyNames.SetEquals(aggregatorNames);
            if (match)
            {
                query.AddOrUpdateComponent(nameof(aggregations), aggregations);
                return query.AsSelf;
            }

            throw new InvalidOperationException($"Added aggregator names did not match property names of {typeof(TAggregations)}.")
            {
                Data =
                {
                    [nameof(aggregatorNames)] = aggregatorNames,
                    [nameof(resultPropertyNames)] = resultPropertyNames
                }
            };
        }

        public static TQuery WithIntervals<TQuery>(this TQuery query, IEnumerable<Interval> intervals)
            where TQuery : IQueryWith.Intervals
        {
            static string ToIsoString(DateTimeOffset t) => t.ToString("o", CultureInfo.InvariantCulture);
            var mapped = intervals.Select(interval => $"{ToIsoString(interval.From)}/{ToIsoString(interval.To)}");
            query.AddOrUpdateComponent(nameof(intervals), mapped);
            return query;
        }

        public static TQuery WithInterval<TQuery>(this TQuery query, Interval interval)
            where TQuery : IQueryWith.Intervals
            => WithIntervals(query, new[] { interval });

        public static TQuery WithOrder<TQuery>(this TQuery query, Order order)
            where TQuery : IQueryWith.Order
        {
            var descending = order is Order.Descending;
            query.AddOrUpdateComponent(nameof(descending), descending);
            return query;
        }

        private static string ToSnake(this string @string)
            => Regex.Replace(Regex.Replace(@string, "(.)([A-Z][a-z]+)", "$1_$2"), "([a-z0-9])([A-Z])", "$1_$2").ToLower();
        private static readonly Dictionary<Granularity, string> granularityMap = Enum
            .GetValues<Granularity>()
            .ToDictionary(granularity => granularity, granularity => granularity.ToString().TrimEnd('s').ToSnake());
        public static TQuery WithGranularity<TQuery>(this TQuery query, Granularity granularity)
            where TQuery : IQueryWith.Granularity
        {
            query.AddOrUpdateComponent(nameof(granularity), granularityMap[granularity]);
            return query;
        }
    }

    public abstract class TimeSeriesQueryBase<TSource> :
        IQueryWith.Order,
        IQueryWith.Intervals
    {
        public string QueryType { get; } = "timeseries";
        Dictionary<string, Action<JsonObject>> IQuery.ComponentWrites { get; } = new();
    }

    public class TimeSeriesQuery<TSource> :
        TimeSeriesQueryBase<TSource>,
        IQueryWith.Filter<TSource, TimeSeriesQuery<TSource>>

    {
    }

    public sealed class TimeSeriesQuery<TSource, TAggregations> :
        TimeSeriesQueryBase<TSource>,
        IQueryWith.Filter<TSource, TimeSeriesQuery<TSource, TAggregations>>,
        IQueryWith.Aggregators<TSource, TAggregations, TimeSeriesQuery<TSource, TAggregations>>
    {
    }
}
