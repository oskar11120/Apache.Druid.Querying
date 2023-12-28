using Apache.Druid.Querying.Internal.QuerySectionFactory;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
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

    public static class Marker 
    {
        public sealed record Dimension;
        public sealed record Dimensions;
    }

    public static class QueryBase<TArguments, TSelf> where TSelf : IQuery<TSelf>
    {
        public abstract class TimeSeries :
            Query,
            IQueryWith.Order,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TimeSeries, TSelf>
        {
            public TimeSeries() : base("timeseries")
            {
            }
        }

        public abstract class TimeSeries<TAggregations> :
            TimeSeries,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
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
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>,
            IQuery<TArguments, TSelf, Marker.Dimension>
        {
            public TopN_() : base("topN")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimension(Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimension>> factory)
                => this.AddOrUpdateSectionWithSectionFactory(nameof(Dimension), factory);

            public TSelf Threshold(int threshold)
                => Self.AddOrUpdateSection(nameof(threshold), threshold);

            public TSelf Metric(Func<QueryElementFactory<TMetricArguments>.MetricSpec, IMetric> factory)
                => Self.AddOrUpdateSection(nameof(Metric), factory(new()));
        }

        public abstract class TopN<TDimension> : TopN_<TDimension, TDimension>
        {
        }

        public abstract class TopN<TDimension, TAggregations> :
            TopN_<TDimension, Dimension_Aggregations<TDimension, TAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class TopN<TDimension, TAggregations, TPostAggregations> :
            TopN_<TDimension, Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }

        public abstract class GroupBy_<TDimensions, TOrderByAndHavingArguments> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.GroupBy, TSelf>,
            IQuery<TArguments, TSelf, Marker.Dimensions>
        {
            public GroupBy_() : base("groupBy")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimensions(Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimensions>> factory)
                => this.AddOrUpdateSectionWithSectionFactory(nameof(Dimensions), factory);

            public TSelf LimitSpec(
                int? limit = null,
                int? offset = null,
                Func<QueryElementFactory<TOrderByAndHavingArguments>.OrderByColumnSpec, IEnumerable<ILimitSpec.OrderBy>>? columns = null)
                => Self.AddOrUpdateSection(nameof(LimitSpec), new LimitSpec(limit, offset, columns?.Invoke(new())));

            public TSelf Having(Func<QueryElementFactory<TOrderByAndHavingArguments>.Having, IHaving> factory)
                => Self.AddOrUpdateSection(nameof(Having), factory(new()));

            public TSelf HavingFilter(Func<QueryElementFactory<TOrderByAndHavingArguments>.Filter, IFilter> factory)
                => Self.AddOrUpdateSection(nameof(Having), new QueryElementFactory<TOrderByAndHavingArguments>.Having().Filter(factory));
        }

        public abstract class GroupBy<TDimensions> : GroupBy_<TDimensions, TDimensions>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations<TDimensions, TAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations, TPostAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }
    }
}
