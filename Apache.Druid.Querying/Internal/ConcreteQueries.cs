using Apache.Druid.Querying.Elements;
using System;

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
        }

        public abstract class TopN<TDimension> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TSource, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>
        {
            private IQuery<TSelf> Self => this;

            public TSelf Dimension(Func<Factory.DimensionSpec<TSource, TDimension>, TDimension> factory)
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

            public TSelf Metric(Func<Factory.TopNMetricSpec<TDimension>, TopNMetric> factory)
            {
                var factory_ = new Factory.TopNMetricSpec<TDimension>();
                var metric = factory(factory_);
                return Metric(metric);
            }

            protected TSelf Metric(TopNMetric metric)
            {
                Self.AddOrUpdateSection(nameof(metric), metric);
                return Self.Unwrapped;
            }
        }

        public abstract class TopN<TDimension, TAggregations> :
            TopN<TDimension>,
            IQueryWith.Aggregations<TSource, TAggregations, TSelf>
        {
            public TSelf Metric(Func<Factory.TopNMetricSpec<TDimension>.WithAggregations<TAggregations>, TopNMetric> factory)
            {
                var factory_ = new Factory.TopNMetricSpec<TDimension>.WithAggregations<TAggregations>();
                var metric = factory(factory_);
                return Metric(metric);
            }
        }

        public abstract class TopN<TDimension, TAggregations, TPostAggregations> :
            TopN<TDimension, TAggregations>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
            public TSelf Metric(Func<Factory.TopNMetricSpec<TDimension>.WithAggregations<TAggregations>.AndPostAggregations<TPostAggregations>, TopNMetric> factory)
            {
                var factory_ = new Factory.TopNMetricSpec<TDimension>.WithAggregations<TAggregations>.AndPostAggregations<TPostAggregations>();
                var metric = factory(factory_);
                return Metric(metric);
            }
        }
    }
}
