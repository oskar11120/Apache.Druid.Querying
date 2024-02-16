namespace Apache.Druid.Querying.Internal.Elements
{
    internal abstract class Having : WithType, IHaving
    {
        public Having(string? type = null) : base(type)
        {
        }

        public sealed class Filter_ : Having
        {
            public Filter_(IFilter filter) : base(nameof(Filter).ToCamelCase())
            {
                Filter = filter;
            }

            public IFilter Filter { get; }
        }
    }
}
