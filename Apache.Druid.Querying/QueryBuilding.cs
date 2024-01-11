using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Globalization;
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
        public IReadOnlyDictionary<string, QuerySectionValueFactory> GetState() => State;
        public void AddOrUpdateSection(string key, QuerySectionValueFactory valueFactory, bool convertKeyToCamelCase = true)
        {
            key = convertKeyToCamelCase ? key.ToCamelCase() : key;
            State.Remove(key);
            State.Add(key, valueFactory);
        }

        public void AddOrUpdateSection<TValue>(string key, Func<IColumnNameMappingProvider, TValue> getValue, bool convertKeyToCamelCase = true)
            => AddOrUpdateSection(key, (options, columnNames) => JsonSerializer.SerializeToNode(getValue(columnNames), options)!, convertKeyToCamelCase);

        public void AddOrUpdateSection<TValue>(string key, TValue value, bool convertKeyToCamelCase = true)
            => AddOrUpdateSection(key, _ => value, convertKeyToCamelCase);
    }

    public abstract class Query : IQuery
    {
        public Query(string? type = null)
        {
            state = new() { ["queryType"] = (_, _) => (type ?? GetType().Name.ToCamelCase())! };
        }

        private readonly Dictionary<string, QuerySectionValueFactory> state;
        Dictionary<string, QuerySectionValueFactory> IQuery.State => state;
    }

    public interface IQuery<TSelf> : IQuery where TSelf : IQuery<TSelf>
    {
        public TSelf Unwrapped => (TSelf)this;
        private IQuery Base => this;

        public new TSelf AddOrUpdateSection(string key, QuerySectionValueFactory valueFactory, bool convertKeyToCamelCase = true)
        {
            Base.AddOrUpdateSection(key, valueFactory, convertKeyToCamelCase);
            return Unwrapped;
        }

        public new TSelf AddOrUpdateSection<TValue>(string key, Func<IColumnNameMappingProvider, TValue> getValue, bool convertKeyToCamelCase = true)
        {
            Base.AddOrUpdateSection(key, getValue, convertKeyToCamelCase);
            return Unwrapped;
        }

        public new TSelf AddOrUpdateSection<TValue>(string key, TValue value, bool convertKeyToCamelCase = true)
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
            IQuery<TArguments, TSelf, Marker.VirtualColumns>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface Aggregations<TArguments, TAggregations, TSelf> :
            IQuery<TArguments, TSelf, Marker.Aggregations>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface PostAggregations<TArguments, TPostAggregations, TSelf>
            : IQuery<TArguments, TSelf, Marker.PostAggregations>
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
                static call =>
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
                            ?? DataType.GetSimple(call.GetMemberSelectorParameter("fieldName").SelectedType).ToString())
                            .ToCamelCase()
                            + call.MethodName,

                        _ => call.MethodName.ToCamelCase()
                    };
                },
                static scalar => scalar.Type == typeof(SimpleDataType)));

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
            var descending = order is OrderDirection.Descending;
            query.AddOrUpdateSection(nameof(descending), descending);
            return query;
        }

        public static TQuery Context<TQuery, TContext>(this IQueryWith.Context<TContext, TQuery> query, TContext context)
            where TQuery : IQuery<TQuery>
            where TContext : Context
            => query.AddOrUpdateSection(nameof(context), context);

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
