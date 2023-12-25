namespace Apache.Druid.Querying.Elements
{
    public abstract class Having : WithType
    {
        public sealed class Filter_ : Having
        {
            public Filter Filter { get; }

            public Filter_(Filter filter)
            {
                Filter = filter;
            }
        }
    }
}
