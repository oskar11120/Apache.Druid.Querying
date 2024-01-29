using Apache.Druid.Querying.Internal.QuerySectionFactory;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Threading;

namespace Apache.Druid.Querying.Internal
{
    public static partial class QueryResultDeserializer
    {
        public interface IArray<TElement, TElementMapper> :
            IQueryResultDeserializer<TElement>
            where TElementMapper : IQueryResultDeserializer<TElement>, new()
        {
            private static readonly IQueryResultDeserializer<TElement> array = new Array<TElement, TElementMapper>();

            IAsyncEnumerable<TElement> IQueryResultDeserializer<TElement>.Deserialize(
                QueryResultDeserializerContext context, CancellationToken token)
                => array.Deserialize(context, token);
        }

        public class WithTimestamp<TValue, TValueMapper>
            : TwoPropertyObject<DateTimeOffset, TValue, TValueMapper, WithTimestamp<TValue>>
            where TValueMapper : IQueryResultDeserializer<TValue>, new()
        {
            public WithTimestamp() : this("result")
            {
            }

            public WithTimestamp(string valuePropertyNameBase)
                : base(nameof(WithTimestamp<TValue>.Timestamp), valuePropertyNameBase, static (t, value) => new(t, value))
            {
            }
        }

        public sealed class GroupByResult<TValue> : WithTimestamp<TValue, Element<TValue>>
        {
            public GroupByResult() : base("event")
            {
            }
        }

        public sealed class ScanResult<TValue, TValueMapper>
            : TwoPropertyObject<string?, TValue, TValueMapper, ScanResult<TValue>>
            where TValueMapper : IQueryResultDeserializer<TValue>, new()
        {
            public ScanResult() : base(nameof(ScanResult<TValue>.SegmentId), "events", static (id, value) => new(id, value))
            {
            }
        }
        public interface ArrayOfObjectsWithTimestamp<TValue, TValueMapper> :
            IArray<WithTimestamp<TValue>, WithTimestamp<TValue, TValueMapper>>
            where TValueMapper : IQueryResultDeserializer<TValue>, new()
        {
        }

        public interface ArrayOfObjectsWithTimestamp<TValue> : ArrayOfObjectsWithTimestamp<TValue, Element<TValue>>
        {
        }

        public interface ArrayOfObjectsWithTimestampAndArray<TValue> : ArrayOfObjectsWithTimestamp<TValue, Element<TValue>>
        {
        }

        public interface ArrayOfGroupByResults<TValue> : IArray<WithTimestamp<TValue>, GroupByResult<TValue>>
        {
        }

        public interface ArrayOfScanResults<TColumns> :
            IArray<
                ScanResult<TColumns>,
                ScanResult<
                    TColumns,
                    Array<TColumns, Element<TColumns>>>>
        {
        }
    }

    public static class Marker
    {
        public sealed record Dimension;
        public sealed record Dimensions;
    }

    public abstract class QueryBase : IQuery, IQueryWithSectionFactoryExpressions, IQueryWith.Intervals
    {
        public QueryBase(string? type = null)
        {
            state = new() { ["queryType"] = (_, _) => (type ?? GetType().Name.ToCamelCase())! };
        }

        private readonly Dictionary<string, QuerySectionValueFactory> state;
        Dictionary<string, QuerySectionValueFactory> IQuery.State => state;
        SectionAtomicity.IProvider.Builder IQueryWithSectionFactoryExpressions.SectionAtomicity { get; } = new();
        IReadOnlyCollection<Interval>? IQueryWith.Intervals.Intervals { get; set; }
    }

    public static class QueryBase<TArguments, TSelf, TResult> where TSelf : IQuery<TSelf>
    {
        public abstract class TimeSeries :
            QueryBase,
            IQueryWith.Order,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TimeSeries, TSelf>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestamp<TResult>
        {
            public TimeSeries() : base("timeseries")
            {
            }
        }

        public abstract class TimeSeries<TAggregations> :
            TimeSeries,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestamp<TAggregations>
        {
        }

        public abstract class TimeSeries<TAggregations, TPostAggregations> :
            TimeSeries<TAggregations>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestamp<Aggregations_PostAggregations<TAggregations, TPostAggregations>>
        {
        }

        private static readonly SectionFactoryJsonMapper.Options dimensionsMapperOptions = new(SectionColumnNameKey: "outputName");
        public abstract class TopN_<TDimension, TMetricArguments> :
            QueryBase,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>,
            IQueryWithSectionFactoryExpressions<TArguments, TSelf, Marker.Dimension>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestampAndArray<TResult>
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
            QueryBase,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.GroupBy, TSelf>,
            IQueryWithSectionFactoryExpressions<TArguments, TSelf, Marker.Dimensions>,
            QueryResultDeserializer.ArrayOfGroupByResults<TResult>
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
            QueryBase,
            IQueryWith.Order,
            IQueryWith.OffsetAndLimit,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.Scan, TSelf>,
            QueryResultDeserializer.ArrayOfScanResults<TResult>
        {
            protected IQuery<TSelf> Self => this;
            int IQueryWith.OffsetAndLimit.Offset { get; set; }
            int IQueryWith.OffsetAndLimit.Limit { get; set; }

            public Scan() : base("scan")
            {
            }

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
                    Self.AddOrUpdateSection("columns", (options, columnNameMappings) =>
                    {
                        var mappings = columnNameMappings.Get<TArguments>();
                        var columnNames = propertyNames;
                        string GetColumnName(string propertyName) => mappings
                            .FirstOrDefault(mapping => mapping.Property == propertyName)
                            ?.ColumnName
                            ?? propertyName;
                        return JsonSerializer.SerializeToNode(propertyNames.Select(GetColumnName), options)!;
                    });
                }
            }
        }
    }
}
