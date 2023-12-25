using Apache.Druid.Querying.Elements;
using System;
using System.Collections.Generic;

namespace Apache.Druid.Querying.Internal
{
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

        public abstract class TopN<TDimension> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TSource, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>
        {
            public TopN() : base("topN")
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

            public TSelf Metric(Func<Factory.MetricSpec<TDimension>, Metric> factory)
            {
                var factory_ = new Factory.MetricSpec<TDimension>();
                var metric = factory(factory_);
                return Metric(metric);
            }

            protected TSelf Metric(Metric metric)
            {
                Self.AddOrUpdateSection(nameof(metric), metric);
                return Self.Unwrapped;
            }
        }

        public abstract class TopN<TDimension, TAggregations> :
            TopN<TDimension>,
            IQueryWith.Aggregations<TSource, TAggregations, TSelf>
        {
            public TSelf Metric(Func<Factory.MetricSpec<TDimension>.WithAggregations<TAggregations>, Metric> factory)
            {
                var factory_ = new Factory.MetricSpec<TDimension>.WithAggregations<TAggregations>();
                var metric = factory(factory_);
                return Metric(metric);
            }
        }

        public abstract class TopN<TDimension, TAggregations, TPostAggregations> :
            TopN<TDimension, TAggregations>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
            public TSelf Metric(Func<Factory.MetricSpec<TDimension>.WithAggregations<TAggregations>.AndPostAggregations<TPostAggregations>, Metric> factory)
            {
                var factory_ = new Factory.MetricSpec<TDimension>.WithAggregations<TAggregations>.AndPostAggregations<TPostAggregations>();
                var metric = factory(factory_);
                return Metric(metric);
            }
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
