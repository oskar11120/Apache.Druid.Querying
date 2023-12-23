namespace Apache.Druid.Querying.Elements
{
    public abstract class TopNMetric : WithType
    {
        public sealed class Numeric : TopNMetric
        {
            public Numeric(string metric)
            {
                Metric = metric;
            }

            public string Metric { get; }

        }

        public sealed class Dimension<TColumn> : TopNMetric
        {
            public Dimension(SortingOrder ordering, TColumn? previousStop)
            {
                Ordering = ordering;
                PreviousStop = previousStop;
            }

            public SortingOrder Ordering { get; }
            public TColumn? PreviousStop { get; }
        }

        public sealed class Inverted : TopNMetric
        {
            public TopNMetric Metric { get; }

            public Inverted(TopNMetric metric)
            {
                Metric = metric;
            }
        }
    }
}
