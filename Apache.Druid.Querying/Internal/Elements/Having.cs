namespace Apache.Druid.Querying.Internal.QuerySectionFactory
{
    internal abstract class Having : WithType, IHaving
    {
        public sealed class Filter_ : Having
        {
            public IFilter Filter { get; }

            public Filter_(IFilter filter)
            {
                Filter = filter;
            }
        }
    }
}
