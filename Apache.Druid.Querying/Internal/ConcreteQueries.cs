using Apache.Druid.Querying.Internal.QuerySectionFactory;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;

namespace Apache.Druid.Querying.Internal
{
    public static class IQueryWithMappedResult<TSource>
    {
        public interface WithTimestampArray<TValue, TValueMapper> :
            IQueryWithSource<TSource>.AndMappedResult<WithTimestamp<TValue>,
            QueryResultMapper.Array<WithTimestamp<TValue>, QueryResultMapper.WithTimestamp<TValue, TValueMapper>>>
            where TValueMapper : IQueryResultMapper<TValue>, new()
        {
        }

        public interface GroupByResultArray<TValue, TValueMapper> :
            IQueryWithSource<TSource>.AndMappedResult<WithTimestamp<TValue>,
            QueryResultMapper.Array<WithTimestamp<TValue>, QueryResultMapper.GroupByResult<TValue, TValueMapper>>>
            where TValueMapper : IQueryResultMapper<TValue>, new()
        {
        }

        public interface Aggregations_PostAggregations_<TAggregations, TPostAggregations> : WithTimestampArray<
            Aggregations_PostAggregations<TAggregations, TPostAggregations>,
            QueryResultMapper.Aggregations_PostAggregations_<TAggregations, TPostAggregations>>
        {
        }

        public interface Dimension_Aggregations_<TDimension, TAggregations> : WithTimestampArray<
            Dimension_Aggregations<TDimension, TAggregations>,
            QueryResultMapper.Array<
                Dimension_Aggregations<TDimension, TAggregations>,
                QueryResultMapper.Dimension_Aggregations_<TDimension, TAggregations>>>
        {
        }

        public interface Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations> : WithTimestampArray<
            Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>,
            QueryResultMapper.Array<
                Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>,
                QueryResultMapper.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>>>
        {
        }

        public interface Dimensions_Aggregations_<TDimensions, TAggregations> : GroupByResultArray<
            Dimensions_Aggregations<TDimensions, TAggregations>,
            QueryResultMapper.Dimensions_Aggregations_<TDimensions, TAggregations>>
        {
        }

        public interface Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations> : GroupByResultArray<
            Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>,
             QueryResultMapper.Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>>
        {
        }

        public interface ScanResult_<TColumns> :
            IQueryWithSource<TSource>
            .AndMappedResult<
                ScanResult<TColumns>,
                QueryResultMapper.Array<
                    ScanResult<TColumns>,
                    QueryResultMapper.ScanResult<
                        TColumns,
                        QueryResultMapper.Array<
                            TColumns,
                            QueryResultMapper.SourceColumns<TColumns>>>>>
        {
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

        public abstract class Scan :
            Query,
            IQueryWith.Order,
            IQueryWith.Intervals,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.Scan, TSelf>
        {
            protected IQuery<TSelf> Self => this;

            public Scan() : base("scan")
            {
            }

            public TSelf Offset(int offset)
                => Self.AddOrUpdateSection(nameof(offset), offset);

            public TSelf Limit(int limit)
                => Self.AddOrUpdateSection(nameof(limit), limit);

            public TSelf BatchSize(int batchSize)
                => Self.AddOrUpdateSection(nameof(batchSize), batchSize);

            public abstract class WithColumns : Scan
            {
                private static readonly string[] propertyNames = typeof(TArguments)
                    .GetProperties()
                    .Select(property => property.Name)
                    .ToArray();

                public WithColumns()
                {
                    Self.AddOrUpdateSection("columns", (options, columnNames)
                        => JsonSerializer.SerializeToNode(propertyNames.Select(columnNames.Get), options)!);
                }
            }
        }
    }
}
