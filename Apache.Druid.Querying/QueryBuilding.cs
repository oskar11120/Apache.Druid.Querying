using static Apache.Druid.Querying.Internal.IQueryWithInternal;
using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Collections.Immutable;

namespace Apache.Druid.Querying
{
    public delegate void OnMapQueryToJson(IQueryWith.State query, JsonObject resultJson);

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

    public static partial class IQueryWith
    {
        public interface State
        {
        }

        public interface Self<out TSelf>
        {
            TSelf Self => (TSelf)this;
        }

        public interface OnMapToJson : State<List<OnMapQueryToJson>>
        {
            void State<List<OnMapQueryToJson>>.CopyFrom(State<List<OnMapQueryToJson>> other)
                => State = other.State?.ToList();
        }

        public interface VirtualColumns<out TArguments, TVirtualColumns, out TSelf> :
            SectionFactoryExpression<TArguments, TVirtualColumns, SectionKind.VirtualColumns>,
            Self<TSelf>
        {
        }

        public interface Aggregations<out TArguments, TAggregations, out TSelf> :
            SectionFactoryExpression<TArguments, TAggregations, SectionKind.Aggregations>,
            Self<TSelf>
        {
        }

        public interface PostAggregations<out TArguments, TPostAggregations, out TSelf> :
            SectionFactoryExpression<TArguments, TPostAggregations, SectionKind.PostAggregations>,
            Self<TSelf>
        {
        }

        public interface Dimesion<out TArguments, TDimension, out TSelf> :
            SectionFactoryExpression<TArguments, TDimension, SectionKind.Dimension>,
            Self<TSelf>
        {
        }

        public interface Dimesions<out TArguments, TDimensions, out TSelf> :
            SectionFactoryExpression<TArguments, TDimensions, SectionKind.Dimensions>,
            Self<TSelf>
        {
        }

        public interface Filter<out TArguments, out TSelf> :
            SectionFactory<IFilter>,
            Self<TSelf>
        {
        }

        public interface Context<TContext, out TSelf> : Self<TSelf>, Section<TContext>
            where TContext : Context
        {
            TContext? Context => State?.Section;
        }

        public interface Intervals : Section<IReadOnlyCollection<Interval>>
        {
            IReadOnlyCollection<Interval> Intervals => Require();
        }

        public interface DescendingFlag : Section<DescendingFlag.InternalState>
        {
            public sealed record InternalState(bool Descending);
            bool Descending => State?.Section.Descending ?? false;
        }

        public interface Order : Section<OrderDirection?>
        {
            OrderDirection? Order => State?.Section;
        }

        public interface Granularity : Section<Querying.Granularity>
        {
            Querying.Granularity Granularity => Require();
        }

        public interface Offset : Section<Offset.InternalState>
        {
            public sealed record InternalState(int Offset);
            int Offset => State?.Section?.Offset ?? 0;
        }

        public interface Limit : Section<Limit.InternalState>
        {
            public sealed record InternalState(int? Limit);
            int? Limit => State?.Section?.Limit;
        }

        public interface OffsetAndLimit : Offset, Limit
        {
        }

        public interface Threshold : Section<Threshold.InternalState>
        {
            public sealed record InternalState(int Threshold);
            int? Threshold => State?.Section?.Threshold;
        }

        public interface Metric<out TArguments, out TSelf> : Self<TSelf>, SectionFactory<IMetric>
        {
        }

        public interface LimitSpec<out TArguments, out TSelf> : Self<TSelf>, SectionFactory<ILimitSpec>
        {
        }

        public interface Having<out TArguments, out TSelf> : Self<TSelf>, SectionFactory<IHaving>
        {
        }

        public interface BatchSize : Section<BatchSize.InternalState>
        {
            public sealed record InternalState(int BatchSize);
            int? BatchSize => State?.Section?.BatchSize;
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

    public delegate TSection QuerySectionFactory<in TElementFactory, out TSection>(TElementFactory factory);

    public static class QueryExtensions
    {
        private delegate void CopyDelegate(IQueryWith.State From, IQueryWith.State To);
        private static readonly ConcurrentDictionary<Type, CopyDelegate> copyCache = new();

        // TODO Optimize
        public static TQuery Copy<TQuery>(this TQuery query) where TQuery : IQueryWith.State
        {
            var runtimeType = query.GetType();
            if (!copyCache.TryGetValue(runtimeType, out var copy))
            {
                var copyFromMethods = query
                    .GetGenericInterfaces(typeof(State<>))
                    .Select(@state => state.GetMethod(nameof(State<None>.CopyFrom), BindingFlags.NonPublic | BindingFlags.Instance))
                    .ToArray();
                copy = (from, to) =>
                {
                    foreach (var method in copyFromMethods)
                        method!.Invoke(to, new[] { from });
                };
                copyCache.TryAdd(runtimeType, copy);
            }

            var @new = (TQuery)Activator.CreateInstance(runtimeType)!;
            copy(query, @new);
            return @new;
        }

        public static TQuery OnMapToJson<TQuery>(this TQuery query, OnMapQueryToJson onMap) 
            where TQuery : IQueryWith.OnMapToJson
        {
            var state = query.State ??= new();
            query.State.Add(onMap);
            return query;
        }

        public static TQuery VirtualColumns<TArguments, TVirtualColumns, TQuery>(
            this IQueryWith.VirtualColumns<TArguments, TVirtualColumns, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IVirtualColumns, TVirtualColumns>> factory)
        {
            query.SetState(nameof(VirtualColumns), factory);
            return query.Self;
        }

        public static TQuery Aggregations<TArguments, TAggregations, TQuery>(
            this IQueryWith.Aggregations<TArguments, TAggregations, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IAggregations, TAggregations>> factory)
        {
            query.SetState(nameof(Aggregations), factory, new(
                MapType: static call => call.MethodName switch
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
                },
                SkipScalarParameter: static scalar => scalar.Type == typeof(SimpleDataType),
                ExpressionColumnNamesKey: "fields"));
            return query.Self;
        }

        public static TQuery PostAggregations<TArguments, TPostAggregations, TQuery>(
            this IQueryWith.PostAggregations<TArguments, TPostAggregations, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IPostAggregators, TPostAggregations>> factory)
        {
            query.SetState(nameof(PostAggregations), factory, new(
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
            return query.Self;
        }

        private static readonly SectionFactoryJsonMapper.Options dimensionsMapperOptions = new(SectionColumnNameKey: "outputName");
        public static TQuery Dimensions<TArguments, TDimensions, TQuery>(
            this IQueryWith.Dimesions<TArguments, TDimensions, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimensions>> factory)
        {
            query.SetState(nameof(Dimensions), factory, dimensionsMapperOptions);
            return query.Self;
        }

        private static readonly SectionFactoryJsonMapper.Options dimensionMapperOptions = dimensionsMapperOptions with { ForceSingle = true };
        public static TQuery Dimension<TArguments, TDimension, TQuery>(
            this IQueryWith.Dimesion<TArguments, TDimension, TQuery> query,
            Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimension>> factory)
        {
            query.SetState(nameof(Dimension), factory, dimensionMapperOptions);
            return query.Self;
        }

        public static TQuery Filter<TArguments, TQuery>(
            this IQueryWith.Filter<TArguments, TQuery> query, Func<QueryElementFactory<TArguments>.Filter, IFilter> factory)
        {
            query.SetState(nameof(Filter), context => factory(new(context.ColumnNames)));
            return query.Self;
        }

        public static TQuery Intervals<TQuery>(this TQuery query, IReadOnlyCollection<Interval> intervals)
            where TQuery : IQueryWith.Intervals
        {
            query.SetState(nameof(Intervals), intervals);
            return query;
        }

        public static TQuery Interval<TQuery>(this TQuery query, Interval interval)
            where TQuery : IQueryWith.Intervals
            => Intervals(query, new[] { interval });

        public static TQuery Descending<TQuery>(this TQuery query, bool descending)
            where TQuery : IQueryWith.DescendingFlag
        {
            query.SetState(nameof(Descending), new(descending), state => state.Descending);
            return query;
        }

        public static TQuery Order<TQuery>(this TQuery query, OrderDirection? order)
            where TQuery : IQueryWith.Order
        {
            query.SetState(nameof(Order), order);
            return query;
        }

        public static TQuery Context<TQuery, TContext>(this IQueryWith.Context<TContext, TQuery> query, TContext context)
            where TContext : Context
        {
            query.SetState(nameof(context), context);
            return query.Self;
        }

        public static TQuery Granularity<TQuery>(this TQuery query, Granularity granularity)
            where TQuery : IQueryWith.Granularity
        {
            query.SetState(nameof(Granularity), granularity);
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
            where TQuery : IQueryWith.Offset
        {
            query.SetState(nameof(Offset), new(offset), section => section.Offset);
            return query;
        }

        public static TQuery Limit<TQuery>(this TQuery query, int limit)
            where TQuery : IQueryWith.Limit
        {
            query.SetState(nameof(Limit), new(limit), section => section.Limit);
            return query;
        }

        public static TQuery Threshold<TQuery>(this TQuery query, int threshold)
            where TQuery : IQueryWith.Threshold
        {
            query.SetState(nameof(threshold), new(threshold), section => section.Threshold);
            return query;
        }

        public static TQuery Metric<TArguments, TQuery>(
            this IQueryWith.Metric<TArguments, TQuery> query,
            Func<QueryElementFactory<TArguments>.MetricSpec, IMetric> factory)
        {
            query.SetState(nameof(Metric), context => factory(new(context.ColumnNames)));
            return query.Self;
        }

        public static TQuery LimitSpec<TArguments, TQuery>(
            this IQueryWith.LimitSpec<TArguments, TQuery> query,
            int? limit = null,
            int? offset = null,
            Func<QueryElementFactory<TArguments>.OrderByColumnSpec, IEnumerable<ILimitSpec.OrderBy>>? columns = null)
        {
            query.SetState(
                nameof(LimitSpec),
                context => new Internal.Elements.LimitSpec(limit, offset, columns?.Invoke(new(context.ColumnNames))));
            return query.Self;
        }

        public static TQuery LimitSpec<TArguments, TQuery>(
            this IQueryWith.LimitSpec<TArguments, TQuery> query,
            int? limit = null,
            int? offset = null,
            Func<QueryElementFactory<TArguments>.OrderByColumnSpec, ILimitSpec.OrderBy>? column = null)
            => query.LimitSpec(limit, offset, column is null ? null : columnNames => new[] { column(columnNames) }.AsEnumerable());

        public static TQuery LimitSpec<TArguments, TQuery>(
            this IQueryWith.LimitSpec<TArguments, TQuery> query,
            int? limit = null,
            int? offset = null)
            => query.LimitSpec(limit, offset, (Func<QueryElementFactory<TArguments>.OrderByColumnSpec, ILimitSpec.OrderBy>?)null);

        public static TQuery Having<TArguments, TQuery>(
            this IQueryWith.Having<TArguments, TQuery> query,
            Func<QueryElementFactory<TArguments>.Having, IHaving> factory)
        {
            query.SetState(nameof(Having), context => factory(new(context.ColumnNames)));
            return query.Self;
        }

        public static TQuery HavingFilter<TArguments, TQuery>(
            this IQueryWith.Having<TArguments, TQuery> query,
            Func<QueryElementFactory<TArguments>.Filter, IFilter> factory)
        {
            query.SetState(nameof(Having), context => new QueryElementFactory<TArguments>.Having(context.ColumnNames).Filter(factory));
            return query.Self;
        }

        public static TQuery BatchSize<TQuery>(this TQuery query, int batchSize)
            where TQuery : IQueryWith.BatchSize
        {
            query.SetState(nameof(batchSize), new(batchSize), section => section.BatchSize);
            return query;
        }
    }
}
