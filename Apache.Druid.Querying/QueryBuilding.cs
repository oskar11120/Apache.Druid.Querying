using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
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

    public delegate JsonNode QuerySectionValueFactory(JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames);

    public interface IQuery
    {
        private protected Dictionary<string, QuerySectionValueFactory> State { get; }
        internal IReadOnlyDictionary<string, QuerySectionValueFactory> GetState() => State;

        internal void AddOrUpdateSection(string key, QuerySectionValueFactory valueFactory, bool convertKeyToCamelCase = true)
        {
            key = convertKeyToCamelCase ? key.ToCamelCase() : key;
            State.Remove(key);
            State.Add(key, valueFactory);
        }

        internal void AddOrUpdateSection<TValue>(string key, Func<IColumnNameMappingProvider, TValue> getValue, bool convertKeyToCamelCase = true)
            => AddOrUpdateSection(key, (options, columnNames) => JsonSerializer.SerializeToNode(getValue(columnNames), options)!, convertKeyToCamelCase);

        internal void AddOrUpdateSection<TValue>(string key, TValue value, bool convertKeyToCamelCase = true)
            => AddOrUpdateSection(key, _ => value, convertKeyToCamelCase);
    }

    public interface IQuery<TSelf> : IQuery where TSelf : IQuery<TSelf>
    {
        public TSelf Unwrapped => (TSelf)this;
        private IQuery Base => this;

        internal new TSelf AddOrUpdateSection(string key, QuerySectionValueFactory valueFactory, bool convertKeyToCamelCase = true)
        {
            Base.AddOrUpdateSection(key, valueFactory, convertKeyToCamelCase);
            return Unwrapped;
        }

        internal new TSelf AddOrUpdateSection<TValue>(string key, Func<IColumnNameMappingProvider, TValue> getValue, bool convertKeyToCamelCase = true)
        {
            Base.AddOrUpdateSection(key, getValue, convertKeyToCamelCase);
            return Unwrapped;
        }

        internal new TSelf AddOrUpdateSection<TValue>(string key, TValue value, bool convertKeyToCamelCase = true)
        {
            Base.AddOrUpdateSection(key, value, convertKeyToCamelCase);
            return Unwrapped;
        }
    }

    public static class IQueryWith
    {
        public static class Marker
        {
            public sealed record VirtualColumns;
            public sealed record Aggregations;
            public sealed record PostAggregations;
        }

        public interface VirtualColumns<TArguments, TVirtualColumns, TSelf> :
            IQueryWithSectionFactoryExpressions<TArguments, TSelf, Marker.VirtualColumns>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface Aggregations<TArguments, TAggregations, TSelf> :
            IQueryWithSectionFactoryExpressions<TArguments, TSelf, Marker.Aggregations>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface PostAggregations<TArguments, TPostAggregations, TSelf>
            : IQueryWithSectionFactoryExpressions<TArguments, TSelf, Marker.PostAggregations>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface Filter<TArguments, TSelf> :
            IQuery<TSelf>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface Context<TContext, TSelf> : IQuery<TSelf>
            where TSelf : IQuery<TSelf>
            where TContext : Context
        {
        }

        public interface Intervals : IQuery
        {
            internal IReadOnlyCollection<Interval>? Intervals { get; set; }

            internal IReadOnlyCollection<Interval> GetIntervals()
                => Intervals is null or { Count: 0 } ?
                    throw new InvalidOperationException($"Mssing required query section: {nameof(Intervals)}.") :
                    Intervals;
        }

        public interface Order : IQuery
        {
        }

        public interface Granularity : IQuery
        {
        }

        public interface OffsetAndLimit : IQuery
        {
            internal int Offset { get; set; }
            internal int Limit { get; set; }
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

    public delegate TSection QuerySectionFactory<TElementFactory, TSection>(TElementFactory factory);

    public static class QueryExtensions
    {
        public static TQuery VirtualColumns<TArguments, TVirtualColumns, TQuery>(
            this IQueryWith.VirtualColumns<TArguments, TVirtualColumns, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IVirtualColumns, TVirtualColumns>> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSectionWithSectionFactory(nameof(VirtualColumns), factory);

        public static TQuery Aggregations<TArguments, TAggregations, TQuery>(
            this IQueryWith.Aggregations<TArguments, TAggregations, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IAggregations, TAggregations>> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSectionWithSectionFactory(nameof(Aggregations), factory, new(
                MapType: static call =>
                {
                    return call.MethodName switch
                    {
                        nameof(QueryElementFactory<TArguments>.IAggregations.Mean) => "doubleMean",

                        nameof(QueryElementFactory<TArguments>.IAggregations.Sum) or
                        nameof(QueryElementFactory<TArguments>.IAggregations.Min) or
                        nameof(QueryElementFactory<TArguments>.IAggregations.Max) or
                        nameof(QueryElementFactory<TArguments>.IAggregations.First) or
                        nameof(QueryElementFactory<TArguments>.IAggregations.Last) or
                        nameof(QueryElementFactory<TArguments>.IAggregations.Any) =>
                            (call.TryGetScalarParameter<SimpleDataType>()?.Value?.ToString()
                            ?? (call.TryGetMemberSelectorParameter("timeColumn") is null ? null : SimpleDataType.String.ToString())
                            ?? DataType.GetSimple(call.GetMemberSelectorParameter("fieldName").SelectedType).ToString())
                            .ToCamelCase()
                            + call.MethodName,

                        _ => call.MethodName.ToCamelCase()
                    };
                },
                SkipScalarParameter: static scalar => scalar.Type == typeof(SimpleDataType),
                ExpressionColumnNamesKey: "fields"));

        public static TQuery PostAggregations<TArguments, TPostAggregations, TQuery>(
            this IQueryWith.PostAggregations<TArguments, TPostAggregations, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IPostAggregators, TPostAggregations>> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSectionWithSectionFactory(nameof(PostAggregations), factory, new(
                ReplaceScalarParameter: static scalar => scalar.Type == typeof(ArithmeticFunction) ?
                    scalar with
                    {
                        Type = typeof(string),
                        Value = scalar.Value switch
                        {
                            ArithmeticFunction.Add => "+",
                            ArithmeticFunction.Subtract => "-",
                            ArithmeticFunction.Multiply => "*",
                            ArithmeticFunction.Divide => "/",
                            ArithmeticFunction.QuotientDivide => "quotient",
                            ArithmeticFunction.Exponentiate => "pow",
                            _ => throw new NotSupportedException(nameof(ArithmeticFunction))
                        }
                    }
                    : scalar));

        public static TQuery Filter<TArguments, TQuery>(this IQueryWith.Filter<TArguments, TQuery> query, Func<QueryElementFactory<TArguments>.Filter, IFilter> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSection(
                nameof(Filter),
                columnNames => factory(new(columnNames)));

        public static TQuery Intervals<TQuery>(this TQuery query, IReadOnlyCollection<Interval> intervals)
            where TQuery : IQueryWith.Intervals
        {
            query.Intervals = intervals;
            query.AddOrUpdateSection(nameof(intervals), intervals.Select(IntervalExtensions.Map));
            return query;
        }

        public static TQuery Interval<TQuery>(this TQuery query, Interval interval)
            where TQuery : IQueryWith.Intervals
            => Intervals(query, new[] { interval });

        public static TQuery Order<TQuery>(this TQuery query, OrderDirection order)
            where TQuery : IQueryWith.Order
        {
            var descending = order is OrderDirection.Descending;
            query.AddOrUpdateSection(nameof(descending), descending);
            return query;
        }

        public static TQuery Context<TQuery, TContext>(this IQueryWith.Context<TContext, TQuery> query, TContext context)
            where TQuery : IQuery<TQuery>
            where TContext : Context
            => query.AddOrUpdateSection(nameof(context), context);

        private static string? MapOrigin(DateTimeOffset? origin)
            => origin is null ? null : JsonSerializer.Serialize(origin);
        private static string ToSnake(this string @string)
            => Regex.Replace(Regex.Replace(@string, "(.)([A-Z][a-z]+)", "$1_$2"), "([a-z0-9])([A-Z])", "$1_$2").ToLower();
        private static readonly Dictionary<Granularity, string> granularityToStringMap = Enum
            .GetValues<Granularity>()
            .ToDictionary(granularity => granularity, granularity => granularity.ToString().TrimEnd('s').ToSnake());
        public static TQuery Granularity<TQuery>(this TQuery query, Granularity granularity)
            where TQuery : IQueryWith.Granularity
        {
            query.AddOrUpdateSection(nameof(granularity), granularityToStringMap[granularity]);
            return query;
        }

        public static TQuery Granularity<TQuery>(this TQuery query, TimeSpan duration, DateTimeOffset? origin = null)
            where TQuery : IQueryWith.Granularity
        {
            query.AddOrUpdateSection(nameof(Granularity), new { type = nameof(duration), duration, origin = MapOrigin(origin) });
            return query;
        }

        public static TQuery Granularity<TQuery>(this TQuery query, string period, string? timeZone = null, DateTimeOffset? origin = null)
           where TQuery : IQueryWith.Granularity
        {
            query.AddOrUpdateSection(nameof(Granularity), new { type = nameof(period), period, timeZone, origin = MapOrigin(origin) });
            return query;
        }

        private static readonly Dictionary<Granularity, string> granularityToPeriodMap = new()
        {
            [Querying.Granularity.Second] = "PT1S",
            [Querying.Granularity.Minute] = "PT1M",
            [Querying.Granularity.FiveMinutes] = "PT5M",
            [Querying.Granularity.TenMinutes] = "PT10M",
            [Querying.Granularity.FifteenMinutes] = "PT15M",
            [Querying.Granularity.ThrityMinutes] = "PT30M",
            [Querying.Granularity.Hour] = "PT1H",
            [Querying.Granularity.SixHours] = "PT6H",
            [Querying.Granularity.EightHours] = "PT8H",
            [Querying.Granularity.Day] = "P1D",
            [Querying.Granularity.Week] = "P1W",
            [Querying.Granularity.Month] = "P1M",
            [Querying.Granularity.Quarter] = "P3M",
            [Querying.Granularity.Year] = "P1Y"
        };
        public static TQuery Granularity<TQuery>(this TQuery query, Granularity granularity, string timeZone, DateTimeOffset? origin = null)
            where TQuery : IQueryWith.Granularity
        {
            if (granularity is Querying.Granularity.All or Querying.Granularity.None)
            {
                query.Granularity(granularity);
                return query;
            }

            var period = granularityToPeriodMap[granularity];
            query.Granularity(period, timeZone, origin);
            return query;
        }

        public static TQuery Offset<TQuery>(this TQuery query, int offset)
            where TQuery : IQueryWith.OffsetAndLimit
        {
            query.Offset = offset;
            query.AddOrUpdateSection(nameof(offset), offset);
            return query;
        }

        public static TQuery Limit<TQuery>(this TQuery query, int limit)
            where TQuery : IQueryWith.OffsetAndLimit
        {
            query.Limit = limit;
            query.AddOrUpdateSection(nameof(limit), limit);
            return query;
        }
    }
}
