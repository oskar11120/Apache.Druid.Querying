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

            public TSelf Dimension(Func<Factory.Dimensions<TSource, TDimension>, TDimension> factory)
            {
                var factory_ = new Factory.Dimensions<TSource, TDimension>();
                var dimension = factory(factory_);
                Self.AddOrUpdateSection(nameof(dimension), dimension);
                return Self.Unwrapped;
            }

            public TSelf Threshold(int threshold)
            {
                Self.AddOrUpdateSection(nameof(threshold), threshold);
                return Self.Unwrapped;
            }


        }
    }


}
