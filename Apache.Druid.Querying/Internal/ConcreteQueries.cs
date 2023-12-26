using Apache.Druid.Querying.Elements;
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
                var resultJson = json.GetProperty(nameof(WithTimestamp<TResult>.Timestamp).ToCamelCase());
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

            public TSelf Dimension(Func<Factory.DimensionSpec<TSource, TDimension>, Dimension> factory)
            {
                var factory_ = new Factory.DimensionSpec<TSource, TDimension>();
                var dimension = factory(factory_);
                Self.AddOrUpdateSection(nameof(dimension), dimension);
                return Self.Unwrapped;
            }

            public TSelf Threshold(int threshold)
            {
                Self.AddOrUpdateSection(nameof(threshold), threshold);
                return Self.Unwrapped;
            }

            public TSelf Metric(Func<Factory.MetricSpec<TMetricArguments>, Metric> factory)
            {
                var factory_ = new Factory.MetricSpec<TMetricArguments>();
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

        public abstract class GroupBy<TDimensions> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TSource, TSelf>,
            IQueryWith.Context<Context.WithVectorization, TSelf> // TODO
        {
            public GroupBy() : base("groupBy")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimensions(Func<Factory.DimensionSpec<TSource, TDimensions>, IEnumerable<Dimension>> factory)
            {
                var factory_ = new Factory.DimensionSpec<TSource, TDimensions>();
                var dimensions = factory(factory_);
                Self.AddOrUpdateSection(nameof(dimensions), dimensions);
                return Self.Unwrapped;
            }

            public TSelf LimitSpec(
                int? limit = null,
                int? offset = null,
                Func<Factory.OrderByColumnSpec<TDimensions>, IEnumerable<LimitSpec.OrderBy>>? columns = null)
            {
                var factory_ = new Factory.OrderByColumnSpec<TDimensions>();
                var limitSpec = new LimitSpec(limit, offset, columns?.Invoke(factory_));
                Self.AddOrUpdateSection(nameof(limitSpec), limitSpec);
                return Self.Unwrapped;
            }
        }
    }
}
