using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace Apache.Druid.Querying
{
    public sealed record Interval(DateTimeOffset From, DateTimeOffset To);

    public enum OrderDirection
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

    public enum SimpleDataType
    {
        String,
        Double,
        Float,
        Long
    }

    public enum ArithmeticFunction
    {
        Add,
        Subtract,
        Multiply,
        Divide,
        QuotientDivide,
        Exponentiate
    }

    public enum SortingOrder
    {
        Lexicographic,
        Alphanumeric,
        Numeric,
        Strlen
    }

    public sealed record QuerySection(Type Type, object? Value);

    public interface IQuery
    {
        private protected Dictionary<string, QuerySection> State { get; }
        public IReadOnlyDictionary<string, QuerySection> GetState() => State;
        internal void AddOrUpdateSection<TSection>(string key, TSection section)
        {
            State.Remove(key);
            State.Add(key, new(typeof(TSection), section));
        }
    }

    public abstract class Query : IQuery
    {
        public Query(string? type = null)
        {
            type ??= GetType().Name.ToCamelCase();
            state = new() { ["queryType"] = new(typeof(string), type) };
        }

        private readonly Dictionary<string, QuerySection> state;
        Dictionary<string, QuerySection> IQuery.State => state;
    }

    public interface IQuery<TSelf> : IQuery where TSelf : IQuery<TSelf>
    {
        public TSelf Unwrapped => (TSelf)this;
    }

    public static class IQueryWith
    {
        public interface VirtualColumns<TArguments, TVirtualColumns, TSelf> : IQuery<TSelf> where TSelf : IQuery<TSelf>
        {
        }

        public interface Filter<TSource, TSelf> : IQuery<TSelf> where TSelf : IQuery<TSelf>
        {
        }

        private static HashSet<string> GetPropertyNames<T>() => typeof(T)
            .GetProperties()
            .Select(property => property.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        public interface Aggregations<TSource, TAggregations, TSelf> : IQuery<TSelf> where TSelf : IQuery<TSelf>
        {
            internal static readonly HashSet<string> AggregationsPropertyNames = GetPropertyNames<TAggregations>();
        }

        public interface PostAggregations<TAggregations, TPostAggregations, TSelf> : IQuery<TSelf> where TSelf : IQuery<TSelf>
        {
            internal static readonly HashSet<string> PostAggregationsPropertyNames = GetPropertyNames<TPostAggregations>();
        }

        public interface Context<TContext, TSelf> : IQuery<TSelf>
            where TSelf : IQuery<TSelf>
            where TContext : Context
        {
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

    public interface IFilter
    {
    }

    public interface IMetric
    {
    }

    public interface IHaving
    {
    }

    public interface ILimitSpec
    {
        public interface OrderBy
        {
        }
    }

    public delegate TSection QuerySectionFactory<TArguments, TSection>(TArguments arguments);

    public static class QueryExtensions
    {
        public static TQuery VirtualColumns<TArguments, TVirtualColumns, TQuery>(
            this IQueryWith.VirtualColumns<TArguments, TVirtualColumns, TQuery> query,
            Expression<Func<QuerySectionFactory<TArguments, TVirtualColumns>>> factory)
            where TQuery : IQuery<TQuery>
        {
            var factory_ = new QueryElementFactory.VirtualColumns<TVirtualColumns>();
            var virtualColumns = factory(factory_);
            query.AddOrUpdateSection(nameof(virtualColumns), virtualColumns);
            return query.Unwrapped;
        }

        public static TQuery Aggregations<TArguments, TQuery, TAggregations>(
            this IQueryWith.Aggregations<TArguments, TAggregations, TQuery> query,
            Expression<Func<QuerySectionFactory<TArguments, TAggregations>>> factory)
            where TQuery : IQuery<TQuery>
        {
            var factory_ = new QueryElementFactory.Aggregators<TArguments, TAggregations>();
            var aggregators = factory(factory_).ToArray();
            var aggregatorNames = aggregators.Select(aggregator => aggregator.Name);
            var propertyNames = IQueryWith.Aggregations<TArguments, TAggregations, TQuery>.AggregationsPropertyNames;
            query.AddOrUpdateSection(nameof(Aggregations).ToCamelCase(), aggregators);
            return query.Unwrapped;
        }

        public static TQuery PostAggregations<TQuery, TArguments, TPostAggregations>(
            this IQueryWith.PostAggregations<TArguments, TPostAggregations, TQuery> query,
            Expression<Func<QuerySectionFactory<TArguments, TArguments>>> factory)
            where TQuery : IQuery<TQuery>
        {
            var factory_ = new QueryElementFactory.PostAggregators<TArguments, TPostAggregations>();
            var postAggregators = factory(factory_).ToArray();
            var postAggregatorNames = postAggregators.Select(aggregator => aggregator.Name);
            var propertyNames = IQueryWith.PostAggregations<TArguments, TPostAggregations, TQuery>.PostAggregationsPropertyNames;
            query.AddOrUpdateSection(nameof(PostAggregations).ToCamelCase(), postAggregators);
            return query.Unwrapped;
        }

        public static TQuery Filter<TArguments, TQuery>(this IQueryWith.Filter<TArguments, TQuery> query, Func<QueryElementFactory<TArguments>.Filter, IFilter> factory)
            where TQuery : IQuery<TQuery>
        {
            var factory_ = new QueryElementFactory<TArguments>.Filter();
            var filter = factory(factory_);
            query.AddOrUpdateSection(nameof(filter), filter);
            return query.Unwrapped;
        }

        public static TQuery Intervals<TQuery>(this TQuery query, IEnumerable<Interval> intervals)
            where TQuery : IQueryWith.Intervals
        {
            static string ToIsoString(DateTimeOffset t) => t.ToString("o", CultureInfo.InvariantCulture);
            var mapped = intervals.Select(interval => $"{ToIsoString(interval.From)}/{ToIsoString(interval.To)}");
            query.AddOrUpdateSection(nameof(intervals), mapped);
            return query;
        }

        public static TQuery Interval<TQuery>(this TQuery query, Interval interval)
            where TQuery : IQueryWith.Intervals
            => Intervals(query, new[] { interval });

        public static TQuery Order<TQuery>(this TQuery query, OrderDirection order)
            where TQuery : IQueryWith.Order
        {
            var descending = order is Querying.OrderDirection.Descending;
            query.AddOrUpdateSection(nameof(descending), descending);
            return query;
        }

        public static TQuery Context<TQuery, TContext>(this IQueryWith.Context<TContext, TQuery> query, TContext context)
            where TQuery : IQuery<TQuery>
            where TContext : Context
        {
            query.AddOrUpdateSection(nameof(context), context);
            return query.Unwrapped;
        }

        private static string ToSnake(this string @string)
            => Regex.Replace(Regex.Replace(@string, "(.)([A-Z][a-z]+)", "$1_$2"), "([a-z0-9])([A-Z])", "$1_$2").ToLower();
        private static readonly Dictionary<Granularity, string> granularityMap = Enum
            .GetValues<Granularity>()
            .ToDictionary(granularity => granularity, granularity => granularity.ToString().TrimEnd('s').ToSnake());
        public static TQuery Granularity<TQuery>(this TQuery query, Granularity granularity)
            where TQuery : IQueryWith.Granularity
        {
            query.AddOrUpdateSection(nameof(granularity), granularityMap[granularity]);
            return query;
        }
    }
}
