namespace Apache.Druid.Querying.Elements
{
    public abstract class Metric : WithType
    {
        public sealed class Numeric : Metric
        {
            public Numeric(string metric)
            {
                Metric = metric;
            }

            public string Metric { get; }

        }

        public sealed class Dimension<TColumn> : Metric
        {
            public Dimension(SortingOrder ordering, TColumn? previousStop)
            {
                Ordering = ordering;
                PreviousStop = previousStop;
            }

            public SortingOrder Ordering { get; }
            public TColumn? PreviousStop { get; }
        }

        public sealed class Inverted : Metric
        {
            public Metric Metric { get; }

            public Inverted(Metric metric)
            {
                Metric = metric;
            }
        }
    }
}
