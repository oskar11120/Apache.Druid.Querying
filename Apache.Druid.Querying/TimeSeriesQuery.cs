using Apache.Druid.Querying.Internal;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Apache.Druid.Querying
{
    public class TimeSeriesQueryBuilder<TSource>
    {
        private Filter? filter;

        public string QueryType { get; } = "timeseries";

        public TimeSeriesQueryBuilder<TSource> WithFilter(Func<Factory<TSource>.Filter, Filter<TSource>> factory)
        {
            var factory_ = new Factory<TSource>.Filter();
            filter = factory(factory_);
            return this;
        }

        public TimeSeriesQueryBuilder<TSource> WithAggregators<TAggregatorsResult>(Func<Factory<TSource>.Filter, Filter<TSource>> factory)
        {
            var factory_ = new FilterFactory<TSource>();
            filter = factory(factory_);
            return this;
        }
    }

    public sealed class TimeSeriesQueryBuilder<TSource, TAggregatorsResult> : TimeSeriesQueryBuilder<TSource>
    {
        private IEnumerable<Aggregator>? aggregators;
    }


}
