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

    public interface IQuery<TSelf> : IQuery
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
    public sealed record SourceWithVirtualColumns<TSource, TVirtualColumns>(TSource Source, TVirtualColumns VirtualColumns);

    public static class IQueryWith
    {
        public interface VirtualColumns<TSource, TQuery> : IQuery<TQuery>
        {
            internal TQueryWithVirtualColumns As<TQueryWithVirtualColumns, TVirtualColumns>() where
                TQueryWithVirtualColumns : IQuery<TQueryWithVirtualColumns>;
        }

        public interface Filter<TSource, TQuery> : IQuery<TQuery>
        {
        }

        private static HashSet<string> GetPropertyNames<T>() => typeof(T)
            .GetProperties()
            .Select(property => property.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        public interface Aggregations<TSource, TAggregations, TSelf> : IQuery<TSelf>
        {
            internal static readonly HashSet<string> AggregationsPropertyNames = GetPropertyNames<TAggregations>();
        }

        public interface PostAggregations<TAggregations, TPostAggregations, TSelf> : IQuery<TSelf>
        {
            internal static readonly HashSet<string> PostAggregationsPropertyNames = GetPropertyNames<TPostAggregations>();
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
        public static TQueryWithVirtualColumns VirtualColumns<TSource, TVirtualColumns, TQuery, TQueryWithVirtualColumns>(
            this IQueryWith.VirtualColumns<TSource, TQuery> query,
            Func<Factory.VirtualColumns<TVirtualColumns>, IEnumerable<VirtualColumn>> factory)
            where TQueryWithVirtualColumns : IQuery<TQueryWithVirtualColumns>
        {
            var factory_ = new Factory.VirtualColumns<TVirtualColumns>();
            var virtualColumns = factory(factory_);
            query.AddOrUpdateComponent(nameof(virtualColumns), virtualColumns);
            return query.As<TQueryWithVirtualColumns, TVirtualColumns>();
        }

        public static TQuery Filter<TSource, TQuery>(this IQueryWith.Filter<TSource, TQuery> query, Func<Factory.Filter<TSource>, Filter> factory)
            where TQuery : IQueryWith.Filter<TSource, TQuery>
        {
            var factory_ = new Factory.Filter<TSource>();
            var filter = factory(factory_);
            query.AddOrUpdateComponent(nameof(filter), filter);
            return query.AsSelf;
        }

        private static void EnsureMatch<TAggregations>(HashSet<string> aggregationPropertyNames, IEnumerable<string> aggregatorNames, string aggregatorsLogName)
        {
            var match = aggregationPropertyNames.SetEquals(aggregatorNames);
            if (!match)
            {
                throw new InvalidOperationException($"Added {aggregatorsLogName} names did not match property names of {typeof(TAggregations)}.")
                {
                    Data =
                    {
                        [aggregatorsLogName + "Names"] = aggregatorNames,
                        ["propertyNames"] = aggregationPropertyNames
                    }
                };
            }
        }
        public static TQuery Aggregations<TSource, TQuery, TAggregations>(
            this IQueryWith.Aggregations<TSource, TAggregations, TQuery> query, Func<Factory.Aggregators<TSource, TAggregations>, IEnumerable<Aggregator>> factory)
            where TQuery : IQueryWith.Aggregations<TSource, TAggregations, TQuery>
        {
            var factory_ = new Factory.Aggregators<TSource, TAggregations>();
            var aggregators = factory(factory_).ToArray();
            var aggregatorNames = aggregators.Select(aggregator => aggregator.Name);
            var propertyNames = IQueryWith.Aggregations<TSource, TAggregations, TQuery>.AggregationsPropertyNames;
            EnsureMatch<TAggregations>(propertyNames, aggregatorNames, nameof(aggregators));
            query.AddOrUpdateComponent(nameof(Aggregations).ToLower(), aggregators);
            return query.AsSelf;
        }

        public static TQuery PostAggregations<TQuery, TAggregations, TPostAggregations>(
            this IQueryWith.PostAggregations<TAggregations, TPostAggregations, TQuery> query, Func<Factory.PostAggregators<TAggregations, TPostAggregations>, IEnumerable<PostAggregator>> factory)
            where TQuery : IQueryWith.PostAggregations<TAggregations, TPostAggregations, TQuery>
        {
            var factory_ = new Factory.PostAggregators<TAggregations, TPostAggregations>();
            var postAggregators = factory(factory_).ToArray();
            var postAggregatorNames = postAggregators.Select(aggregator => aggregator.Name);
            var propertyNames = IQueryWith.PostAggregations<TAggregations, TPostAggregations, TQuery>.PostAggregationsPropertyNames;
            EnsureMatch<TPostAggregations>(propertyNames, postAggregatorNames, nameof(postAggregators));
            query.AddOrUpdateComponent(nameof(PostAggregations).ToLower(), postAggregators);
            return query.AsSelf;
        }

        public static TQuery Intervals<TQuery>(this TQuery query, IEnumerable<Interval> intervals)
            where TQuery : IQueryWith.Intervals
        {
            static string ToIsoString(DateTimeOffset t) => t.ToString("o", CultureInfo.InvariantCulture);
            var mapped = intervals.Select(interval => $"{ToIsoString(interval.From)}/{ToIsoString(interval.To)}");
            query.AddOrUpdateComponent(nameof(intervals), mapped);
            return query;
        }

        public static TQuery Interval<TQuery>(this TQuery query, Interval interval)
            where TQuery : IQueryWith.Intervals
            => Intervals(query, new[] { interval });

        public static TQuery Order<TQuery>(this TQuery query, Order order)
            where TQuery : IQueryWith.Order
        {
            var descending = order is Querying.Order.Descending;
            query.AddOrUpdateComponent(nameof(descending), descending);
            return query;
        }

        private static string ToSnake(this string @string)
            => Regex.Replace(Regex.Replace(@string, "(.)([A-Z][a-z]+)", "$1_$2"), "([a-z0-9])([A-Z])", "$1_$2").ToLower();
        private static readonly Dictionary<Granularity, string> granularityMap = Enum
            .GetValues<Granularity>()
            .ToDictionary(granularity => granularity, granularity => granularity.ToString().TrimEnd('s').ToSnake());
        public static TQuery Granularity<TQuery>(this TQuery query, Granularity granularity)
            where TQuery : IQueryWith.Granularity
        {
            query.AddOrUpdateComponent(nameof(granularity), granularityMap[granularity]);
            return query;
        }
    }

    public static class TimeSeriesQuery<TSource>
    {
        public abstract class AfterSpecifyingVirtualColumns<TNewSource, TSelf> :
            IQueryWith.Order,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TNewSource, TSelf>
        {
            public string QueryType { get; } = "timeseries";
            Dictionary<string, Action<JsonObject>> IQuery.ComponentWrites { get; } = new();

            
            public class WithAggregations<TAggregations> : AfterSpecifyingAggregations<TNewSource, TAggregations, WithAggregations<TAggregations>>
            {
            }
        }

        public abstract class AfterSpecifyingAggregations<TNewSource, TAggregations, TSelf> :
            AfterSpecifyingVirtualColumns<TNewSource, TSelf>,
            IQueryWith.Aggregations<TNewSource, TAggregations, TSelf>
        {
            public class WithPostAggregations<TPostAggregations> :
                AfterSpecifyingAggregations<TNewSource, TAggregations, WithPostAggregations<TPostAggregations>>,
                IQueryWith.PostAggregations<TAggregations, TPostAggregations, WithPostAggregations<TPostAggregations>>
            {
            }
        }


        public class WithVirtualColumns<TVirtualColumns> :
            AfterSpecifyingVirtualColumns<SourceWithVirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>
        {
        }

        public class WithNoVirtualColumns : AfterSpecifyingVirtualColumns<TSource, WithNoVirtualColumns>
        {
        }
    }
}
