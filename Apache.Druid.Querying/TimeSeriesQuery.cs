using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using static System.Net.Mime.MediaTypeNames;
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

    public static class IQueryWith
    {
        public interface Filter<TSource, TQuery> : IQuery<TSource, TQuery>
        {
        }

        public interface Aggregators<TAggregatorsResult> : IQuery
        {
            internal static readonly HashSet<string> ResultPropertyNames = typeof(TAggregatorsResult)
                .GetProperties()
                .Select(property => property.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            internal interface AndPostAggregators<TPostAggregatorsResult> : Aggregators<TAggregatorsResult>
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
        public static TQuery WithFilter<TSource, TQuery>(this IQuery<TSource, TQuery> query, Func<Factory<TSource>.Filter, Filter> factory)
            where TQuery : IQueryWith.Filter<TSource, TQuery>
        {
            var factory_ = new Factory<TSource>.Filter();
            var filter = factory(factory_);
            query.AddOrUpdateComponent(nameof(filter), filter);
            return query.AsSelf;
        }

        public static TQuery WithAggregators<TSource, TQuery, TAggregatorsResult>(
            this TQuery query, Func<Factory<TSource>.Aggregators<TAggregatorsResult>, IEnumerable<Aggregator>> factory)
            where TQuery : IQueryWith.Aggregators<TAggregatorsResult>
        {
            var factory_ = new Factory<TSource>.Aggregators<TAggregatorsResult>();
            var aggregations = factory(factory_).ToArray();
            var aggregatorNames = aggregations.Select(aggregator => aggregator.Name);
            var resultPropertyNames = IQueryWith.Aggregators<TAggregatorsResult>.ResultPropertyNames;
            var match = resultPropertyNames.SetEquals(aggregatorNames);
            if (match)
            {
                query.AddOrUpdateComponent(nameof(aggregations), aggregations);
                return query;
            }

            throw new InvalidOperationException($"Added aggregator names did not match property names of {typeof(TAggregatorsResult)}.")
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

    public class TimeSeriesQuery<TSource> :
        IQueryWith.Filter<TSource, TimeSeriesQuery<TSource>>,
        IQueryWith.Order,
        IQueryWith.Intervals
    {
        public string QueryType { get; } = "timeseries";
        Dictionary<string, Action<JsonObject>> IQuery.ComponentWrites { get; } = new();
    }

    public sealed class TimeSeriesQuery<TSource, TAggregatorsResult> :
        TimeSeriesQuery<TSource>,
        IQueryWith.Aggregators<TAggregatorsResult>
    {
    }
}
