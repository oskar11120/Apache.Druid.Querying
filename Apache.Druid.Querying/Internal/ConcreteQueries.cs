using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System;
using System.Collections.Generic;
using System.Text.Json;

namespace Apache.Druid.Querying.Internal
{
    internal static class IQueryWithMappedResult
    {
        public interface WithTimestamp_<TResult> : IQueryWithMappedResult<WithTimestamp<TResult>>
        {
            TResult MapResult(JsonElement from, JsonSerializerOptions options);

            WithTimestamp<TResult> IQueryWithMappedResult<WithTimestamp<TResult>>.Map(JsonElement json, JsonSerializerOptions options)
            {
                var t = json
                    .GetProperty(nameof(WithTimestamp<TResult>.Timestamp).ToCamelCase())
                    .Deserialize<DateTimeOffset>(options);
                var resultJson = json.GetProperty(nameof(WithTimestamp<TResult>.Result).ToCamelCase());
                var result = MapResult(resultJson, options);
                return new(t, result);
            }
        }

        public interface Aggregations_PostAggregations_<TAggregations, TPostAggregations>
            : WithTimestamp_<Aggregations_PostAggregations<TAggregations, TPostAggregations>>
        {
            Aggregations_PostAggregations<TAggregations, TPostAggregations> WithTimestamp_<Aggregations_PostAggregations<TAggregations, TPostAggregations>>.MapResult(
                JsonElement json, JsonSerializerOptions options) =>
                new(
                    json.Deserialize<TAggregations>(options)!,
                    json.Deserialize<TPostAggregations>(options)!);
        }

        public interface Dimension_Aggregations_<TDimensions, TAggregations>
            : WithTimestamp_<Dimension_Aggregations<TDimensions, TAggregations>>
        {
            Dimension_Aggregations<TDimensions, TAggregations> WithTimestamp_<Dimension_Aggregations<TDimensions, TAggregations>>.MapResult(
                JsonElement json, JsonSerializerOptions options)
                => new(
                    json.Deserialize<TDimensions>(options)!,
                    json.Deserialize<TAggregations>(options)!);
        }

        public interface Dimension_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
             : WithTimestamp_<Dimension_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
        {
            Dimension_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations> WithTimestamp_<Dimension_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>.MapResult(
                JsonElement json, JsonSerializerOptions options) => new(
                json.Deserialize<TDimensions>(options)!,
                json.Deserialize<TAggregations>(options)!,
                json.Deserialize<TPostAggregations>(options)!);
        }

        public interface Dimensions_Aggregations_<TDimensions, TAggregations>
            : WithTimestamp_<Dimensions_Aggregations<TDimensions, TAggregations>>
        {
            Dimensions_Aggregations<TDimensions, TAggregations> WithTimestamp_<Dimensions_Aggregations<TDimensions, TAggregations>>.MapResult(
                JsonElement json, JsonSerializerOptions options)
                => new(
                    json.Deserialize<TDimensions>(options)!,
                    json.Deserialize<TAggregations>(options)!);
        }

        public interface Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
             : WithTimestamp_<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
        {
            Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations> WithTimestamp_<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>.MapResult(
                JsonElement json, JsonSerializerOptions options) => new(
                json.Deserialize<TDimensions>(options)!,
                json.Deserialize<TAggregations>(options)!,
                json.Deserialize<TPostAggregations>(options)!);
        }
    }

    public static class QueryBase<TSource, TSelf> where TSelf : IQuery<TSelf>
    {
        public abstract class TimeSeries :
            Query,
            IQueryWith.Order,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TSource, TSelf>,
            IQueryWith.Context<QueryContext.TimeSeries, TSelf>
        {
            public TimeSeries() : base("timeseries")
            {
            }
        }

        public abstract class TimeSeries<TAggregations> :
            TimeSeries,
            IQueryWith.Aggregations<TSource, TAggregations, TSelf>
        {
        }

        public abstract class TimeSeries<TAggregations, TPostAggregations> :
            TimeSeries<TAggregations>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }

        public abstract class TopN_<TDimension, TMetricArguments> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TSource, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>
        {
            public TopN_() : base("topN")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimension(Func<QuerySectionFactory.DimensionSpec<TSource, TDimension>, Dimension> factory)
            {
                var factory_ = new QuerySectionFactory.DimensionSpec<TSource, TDimension>();
                var dimension = factory(factory_);
                Self.AddOrUpdateSection(nameof(dimension), dimension);
                return Self.Unwrapped;
            }

            public TSelf Threshold(int threshold)
            {
                Self.AddOrUpdateSection(nameof(threshold), threshold);
                return Self.Unwrapped;
            }

            public TSelf Metric(Func<QuerySectionFactory.MetricSpec<TMetricArguments>, Metric> factory)
            {
                var factory_ = new QuerySectionFactory.MetricSpec<TMetricArguments>();
                var metric = factory(factory_);
                return Metric(metric);
            }

            protected TSelf Metric(Metric metric)
            {
                Self.AddOrUpdateSection(nameof(metric), metric);
                return Self.Unwrapped;
            }
        }

        public abstract class TopN<TDimension> : TopN_<TDimension, TDimension>
        {
        }

        public abstract class TopN<TDimension, TAggregations> :
            TopN_<TDimension, Dimension_Aggregations<TDimension, TAggregations>>,
            IQueryWith.Aggregations<TSource, TAggregations, TSelf>
        {
        }

        public abstract class TopN<TDimension, TAggregations, TPostAggregations> :
            TopN_<TDimension, Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TSource, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }

        public abstract class GroupBy_<TDimensions, TOrderByAndHavingArguments> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TSource, TSelf>,
            IQueryWith.Context<QueryContext.GroupBy, TSelf>
        {
            public GroupBy_() : base("groupBy")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimensions(Func<QuerySectionFactory.DimensionSpec<TSource, TDimensions>, IEnumerable<Dimension>> factory)
            {
                var factory_ = new QuerySectionFactory.DimensionSpec<TSource, TDimensions>();
                var dimensions = factory(factory_);
                Self.AddOrUpdateSection(nameof(dimensions), dimensions);
                return Self.Unwrapped;
            }

            public TSelf LimitSpec(
                int? limit = null,
                int? offset = null,
                Func<QuerySectionFactory.OrderByColumnSpec<TOrderByAndHavingArguments>, IEnumerable<LimitSpec.OrderBy>>? columns = null)
            {
                var factory_ = new QuerySectionFactory.OrderByColumnSpec<TOrderByAndHavingArguments>();
                var limitSpec = new LimitSpec(limit, offset, columns?.Invoke(factory_));
                Self.AddOrUpdateSection(nameof(limitSpec), limitSpec);
                return Self.Unwrapped;
            }

            public TSelf Having(Func<QuerySectionFactory.Having<TOrderByAndHavingArguments>, Having> factory)
            {
                var factory_ = new QuerySectionFactory.Having<TOrderByAndHavingArguments>();
                var having = factory(factory_);
                Self.AddOrUpdateSection(nameof(having), having);
                return Self.Unwrapped;
            }

            public TSelf HavingFilter(Func<QuerySectionFactory.Filter<TOrderByAndHavingArguments>, Filter> factory)
            {
                var factory_ = new QuerySectionFactory.Having<TOrderByAndHavingArguments>();
                var having = factory_.Filter(factory);
                Self.AddOrUpdateSection(nameof(having), having);
                return Self.Unwrapped;
            }
        }

        public abstract class GroupBy<TDimensions> : GroupBy_<TDimensions, TDimensions>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations<TDimensions, TAggregations>>,
            IQueryWith.Aggregations<TSource, TAggregations, TSelf>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations, TPostAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TSource, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }
    }
}
