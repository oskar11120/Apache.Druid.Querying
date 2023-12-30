using Apache.Druid.Querying.Internal.QuerySectionFactory;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text.Json;

namespace Apache.Druid.Querying.Internal
{
    public static class IQueryWithMappedResult
    {
        public interface WithTimestamp<TResult, TResultMapper>
            : IQueryWithMappedResult<WithTimestamp<TResult>, QueryResultMapper.WithTimestamp<TResult, TResultMapper>>
            where TResultMapper : IQueryResultMapper<TResult>, new()
        {
        }

        public static class WithTimestamp
        {
            public interface Aggregations_PostAggregations_<TAggregations, TPostAggregations> : WithTimestamp<
                Aggregations_PostAggregations<TAggregations, TPostAggregations>,
                QueryResultMapper.Aggregations_PostAggregations_<TAggregations, TPostAggregations>>
            {
            }

            public interface Dimension_Aggregations_<TDimension, TAggregations> : WithTimestamp<
                List<Dimension_Aggregations<TDimension, TAggregations>>,
                QueryResultMapper.List<
                    Dimension_Aggregations<TDimension, TAggregations>,
                    QueryResultMapper.Dimension_Aggregations_<TDimension, TAggregations>>>
            {
            }

            public interface Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations> : WithTimestamp<
                List<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>,
                QueryResultMapper.List<
                    Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>,
                    QueryResultMapper.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>>>
            {
            }
        }
    }

    public static class QueryResultMapper
    {
        public class WithTimestamp<TResult, TResultMapper> :
            IQueryResultMapper<WithTimestamp<TResult>>
            where TResultMapper : IQueryResultMapper<TResult>, new()
        {
            private static readonly IQueryResultMapper<TResult> mapper = new TResultMapper();

            public WithTimestamp<TResult> Map(JsonElement json, JsonSerializerOptions options)
            {
                var t = json
                    .GetProperty(nameof(WithTimestamp<TResult>.Timestamp).ToCamelCase())
                    .Deserialize<DateTimeOffset>(options);
                var resultJson = json.GetProperty(nameof(WithTimestamp<TResult>.Result).ToCamelCase());
                var result = mapper.Map(resultJson, options);
                return new(t, result);
            }
        }

        public class List<TValue, TValueMapper> :
            IQueryResultMapper<List<TValue>>
            where TValueMapper : IQueryResultMapper<TValue>, new()
        {
            private static readonly IQueryResultMapper<TValue> mapper = new TValueMapper();

            public List<TValue> Map(JsonElement json, JsonSerializerOptions options)
            {
                var result = new List<TValue>(json.GetArrayLength());
                foreach (var value in json.EnumerateArray())
                    result.Add(mapper.Map(value, options));
                return result;
            }
        }

        public class Aggregations_PostAggregations_<TAggregations, TPostAggregations> :
            IQueryResultMapper<Aggregations_PostAggregations<TAggregations, TPostAggregations>>
        {
            public Aggregations_PostAggregations<TAggregations, TPostAggregations> Map(JsonElement json, JsonSerializerOptions options)
            => new(
                json.Deserialize<TAggregations>(options)!,
                json.Deserialize<TPostAggregations>(options)!);
        }

        public class Dimension_Aggregations_<TDimension, TAggregations> :
            IQueryResultMapper<Dimension_Aggregations<TDimension, TAggregations>>
        {
            public Dimension_Aggregations<TDimension, TAggregations> Map(JsonElement json, JsonSerializerOptions options)
            => new(
                json.Deserialize<TDimension>(options)!,
                json.Deserialize<TAggregations>(options)!);
        }


        public class Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
             : IQueryResultMapper<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>
        {
            public Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations> Map(JsonElement json, JsonSerializerOptions options)
            => new(
                json.Deserialize<TDimension>(options)!,
                json.Deserialize<TAggregations>(options)!,
                json.Deserialize<TPostAggregations>(options)!);
        }

        public class Dimensions_Aggregations_<TDimensions, TAggregations>
            : IQueryResultMapper<Dimensions_Aggregations<TDimensions, TAggregations>>
        {
            public Dimensions_Aggregations<TDimensions, TAggregations> Map(JsonElement json, JsonSerializerOptions options)
            => new(
                json.Deserialize<TDimensions>(options)!,
                json.Deserialize<TAggregations>(options)!);
        }

        public class Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
             : IQueryResultMapper<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
        {
            public Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations> Map(JsonElement json, JsonSerializerOptions options)
            => new(
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

        private static readonly SectionFactoryJsonMapper.Options dimensionsMapperOptions = new(SectionColumnNameKey: "outputName");
        public abstract class TopN_<TDimension, TMetricArguments> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>,
            IQuery<TArguments, TSelf, Marker.Dimension>
        {
            private static readonly SectionFactoryJsonMapper.Options mapperOptions = dimensionsMapperOptions with { ForceSingle = true };

            public TopN_() : base("topN")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimension(Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimension>> factory)
                => this.AddOrUpdateSectionWithSectionFactory(nameof(Dimension), factory, mapperOptions);

            public TSelf Threshold(int threshold)
                => Self.AddOrUpdateSection(nameof(threshold), threshold);

            public TSelf Metric(Func<QueryElementFactory<TMetricArguments>.MetricSpec, IMetric> factory)
                => Self.AddOrUpdateSection(nameof(Metric), columnNames => factory(new(columnNames)));
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
                => this.AddOrUpdateSectionWithSectionFactory(nameof(Dimensions), factory, dimensionsMapperOptions);

            public TSelf LimitSpec(
                int? limit = null,
                int? offset = null,
                Func<QueryElementFactory<TOrderByAndHavingArguments>.OrderByColumnSpec, IEnumerable<ILimitSpec.OrderBy>>? columns = null)
                => Self.AddOrUpdateSection(nameof(LimitSpec), columnNames => new LimitSpec(limit, offset, columns?.Invoke(new(columnNames))));

            public TSelf Having(Func<QueryElementFactory<TOrderByAndHavingArguments>.Having, IHaving> factory)
                => Self.AddOrUpdateSection(nameof(Having), columnNames => factory(new(columnNames)));

            public TSelf HavingFilter(Func<QueryElementFactory<TOrderByAndHavingArguments>.Filter, IFilter> factory)
                => Self.AddOrUpdateSection(nameof(Having), columnNames => new QueryElementFactory<TOrderByAndHavingArguments>.Having(columnNames).Filter(factory));
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
