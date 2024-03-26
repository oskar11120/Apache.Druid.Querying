using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying
{
    public sealed record Interval(DateTimeOffset From, DateTimeOffset To);

    public enum OrderDirection
    {
        Ascending,
        Descending
    }

    public enum SimpleGranularity
    {
        None,
        Second,
        Minute,
        FiveMinutes,
        TenMinutes,
        FifteenMinutes,
        ThirtyMinutes,
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

    public sealed class Granularity
    {
        public Granularity(SimpleGranularity? simple, TimeSpan? duration, string? period, string? timeZone, DateTimeOffset? origin)
        {
            Simple = simple;
            Duration = duration;
            Period = period;
            TimeZone = timeZone;
            Origin = origin;
            EnsureValid();
        }

        public static Granularity NewSimple(SimpleGranularity simple, string? timeZone = null, DateTimeOffset? origin = null)
            => new(simple, null, null, timeZone, origin);
        public static Granularity NewDuration(TimeSpan duration, DateTimeOffset? origin = null)
            => new(null, duration, null, null, origin);
        public static Granularity NewPeriod(string period, string? timeZone = null, DateTimeOffset? origin = null)
            => new(null, null, period, timeZone, origin);

        public SimpleGranularity? Simple { get; }
        public TimeSpan? Duration { get; }
        public string? Period { get; }
        public string? TimeZone { get; }
        public DateTimeOffset? Origin { get; }

        public TResult Switch<TArguments, TResult>(
            TArguments arguments,
            Func<SimpleGranularity, Granularity, TArguments, TResult> ifSimple,
            Func<TimeSpan, Granularity, TArguments, TResult> ifDuration,
            Func<string, Granularity, TArguments, TResult> ifPeriod)
            => (Simple, Duration, Period) switch
            {
                (SimpleGranularity simple, _, _) => ifSimple(simple, this, arguments),
                (_, TimeSpan duration, _) => ifDuration(duration, this, arguments),
                (_, _, string period) => ifPeriod(period, this, arguments),
                _ => throw new NotSupportedException()
            };

        public TResult Switch<TResult>(
            Func<SimpleGranularity, Granularity, TResult> ifSimple,
            Func<TimeSpan, Granularity, TResult> ifDuration,
            Func<string, Granularity, TResult> ifPeriod)
            => Switch(
                (ifSimple, ifDuration, ifPeriod),
                static (simple, granularity, args) => args.ifSimple(simple, granularity),
                static (duration, granularity, args) => args.ifDuration(duration, granularity),
                static (period, granularity, args) => args.ifPeriod(period, granularity));

        private void EnsureValid()
        {
            InvalidOperationException Invalid(string message) => new(message) { Data = { [nameof(Granularity)] = this } };
            var sum =
                Convert.ToInt32(Simple is not null) +
                Convert.ToInt32(Duration is not null) +
                Convert.ToInt32(Period is not null);
            if (sum is not 1)
                throw Invalid($"{nameof(Granularity)} has to be either {nameof(Simple)}, {nameof(Duration)} or {nameof(Period)}.");
            if (Duration is not null && TimeZone is not null)
                throw Invalid($"{nameof(Duration)} granularity does not support {nameof(TimeZone)}s.");
        }
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

    internal delegate JsonNode QuerySectionValueFactory(JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames);

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
            public sealed record Dimension;
            public sealed record Dimensions;
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

        public interface Dimesion<TArguments, TDimension, TSelf> :
            IQueryWithSectionFactoryExpressions<TArguments, TSelf, Marker.Dimension>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface Dimesions<TArguments, TDimensions, TSelf> :
            IQueryWithSectionFactoryExpressions<TArguments, TSelf, Marker.Dimensions>
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

            IReadOnlyCollection<Interval> GetIntervals()
                => Intervals is null or { Count: 0 } ?
                    throw new InvalidOperationException($"Mssing required query section: {nameof(Intervals)}.")
                    { Data = { ["query"] = this } } :
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

            int GetOffset() => Offset;
            int GetLimit() => Limit;
        }

        public interface Threshold : IQuery
        {
        }

        public interface Metric<TArguments, TSelf> : IQuery<TSelf>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface LimitSpec<TArguments, TSelf> : IQuery<TSelf>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface Having<TArguments, TSelf> : IQuery<TSelf>
            where TSelf : IQuery<TSelf>
        {
        }

        public interface BatchSize : IQuery
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
                    : scalar,
                SkipScalarParameter: static scalar => scalar.Name is "finalizing",
                MapType: static call =>
                    call.MethodName is nameof(QueryElementFactory<TArguments>.IPostAggregators.FieldAccess) &&
                    call.TryGetScalarParameter<bool>() is { Name: "finalizing", Value: true } ?
                    "finalizingFieldAccess" : call.MethodName.ToCamelCase()));

        private static readonly SectionFactoryJsonMapper.Options dimensionsMapperOptions = new(SectionColumnNameKey: "outputName");
        public static TQuery Dimensions<TArguments, TDimensions, TQuery>(
            this IQueryWith.Dimesions<TArguments, TDimensions, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimensions>> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSectionWithSectionFactory(nameof(Dimensions), factory, dimensionsMapperOptions);

        private static readonly SectionFactoryJsonMapper.Options dimensionMapperOptions = dimensionsMapperOptions with { ForceSingle = true };
        public static TQuery Dimension<TArguments, TDimension, TQuery>(
            this IQueryWith.Dimesion<TArguments, TDimension, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimension>> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSectionWithSectionFactory(nameof(Dimension), factory, dimensionMapperOptions);

        public static TQuery Filter<TArguments, TQuery>(this IQueryWith.Filter<TArguments, TQuery> query, Func<QueryElementFactory<TArguments>.Filter, IFilter> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSection(
                nameof(Filter),
                columnNames => factory(new(columnNames)));

        public static TQuery Intervals<TQuery>(this TQuery query, IReadOnlyCollection<Interval> intervals)
            where TQuery : IQueryWith.Intervals
        {
            query.Intervals = intervals;
            query.AddOrUpdateSection(nameof(intervals), intervals);
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

        public static TQuery Granularity<TQuery>(this TQuery query, Granularity granularity)
            where TQuery : IQueryWith.Granularity
        {
            query.AddOrUpdateSection(nameof(Granularity), granularity);
            return query;
        }

        public static TQuery Granularity<TQuery>(this TQuery query, SimpleGranularity simple, string? timeZone = null, DateTimeOffset? origin = null)
            where TQuery : IQueryWith.Granularity
            => query.Granularity(Querying.Granularity.NewSimple(simple, timeZone, origin));

        public static TQuery Granularity<TQuery>(this TQuery query, TimeSpan duration, DateTimeOffset? origin = null)
            where TQuery : IQueryWith.Granularity
            => query.Granularity(Querying.Granularity.NewDuration(duration, origin));

        public static TQuery Granularity<TQuery>(this TQuery query, string period, string? timeZone = null, DateTimeOffset? origin = null)
           where TQuery : IQueryWith.Granularity
            => query.Granularity(Querying.Granularity.NewPeriod(period, timeZone, origin));

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

        public static TQuery Threshold<TQuery>(this TQuery query, int threshold)
            where TQuery : IQueryWith.Threshold
        {
            query.AddOrUpdateSection(nameof(threshold), threshold);
            return query;
        }

        public static TQuery Metric<TArguments, TQuery>(
            this IQueryWith.Metric<TArguments, TQuery> query,
            Func<QueryElementFactory<TArguments>.MetricSpec, IMetric> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSection(nameof(Metric), columnNames => factory(new(columnNames)));

        public static TQuery LimitSpec<TArguments, TQuery>(
            this IQueryWith.LimitSpec<TArguments, TQuery> query,
            int? limit = null,
            int? offset = null,
            Func<QueryElementFactory<TArguments>.OrderByColumnSpec, IEnumerable<ILimitSpec.OrderBy>>? columns = null)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSection(nameof(LimitSpec), columnNames => new Internal.Elements.LimitSpec(limit, offset, columns?.Invoke(new(columnNames))));

        public static TQuery LimitSpec<TArguments, TQuery>(
            this IQueryWith.LimitSpec<TArguments, TQuery> query,
            int? limit = null,
            int? offset = null,
            Func<QueryElementFactory<TArguments>.OrderByColumnSpec, ILimitSpec.OrderBy>? column = null)
            where TQuery : IQuery<TQuery>
            => query.LimitSpec(limit, offset, column is null ? null : columnNames => new[] { column(columnNames) }.AsEnumerable());

        public static TQuery LimitSpec<TArguments, TQuery>(
            this IQueryWith.LimitSpec<TArguments, TQuery> query,
            int? limit = null,
            int? offset = null)
            where TQuery : IQuery<TQuery>
            => query.LimitSpec(limit, offset, (Func<QueryElementFactory<TArguments>.OrderByColumnSpec, ILimitSpec.OrderBy>?)null);

        public static TQuery Having<TArguments, TQuery>(
            this IQueryWith.Having<TArguments, TQuery> query,
            Func<QueryElementFactory<TArguments>.Having, IHaving> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSection(nameof(Having), columnNames => factory(new(columnNames)));

        public static TQuery HavingFilter<TArguments, TQuery>(
            this IQueryWith.Having<TArguments, TQuery> query,
            Func<QueryElementFactory<TArguments>.Filter, IFilter> factory)
            where TQuery : IQuery<TQuery>
            => query.AddOrUpdateSection(nameof(Having), columnNames => new QueryElementFactory<TArguments>.Having(columnNames).Filter(factory));

        public static TQuery BatchSize<TQuery>(this TQuery query, int batchSize)
            where TQuery : IQueryWith.OffsetAndLimit
        {
            query.AddOrUpdateSection(nameof(batchSize), batchSize);
            return query;
        }
    }
}
