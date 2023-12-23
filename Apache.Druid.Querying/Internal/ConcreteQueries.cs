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
            protected TimeSeries() : base("timeseries")
            {
            }
        }
    }
}
