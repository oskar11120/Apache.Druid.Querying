namespace Apache.Druid.Querying.Internal.QuerySectionFactory
{
    internal abstract class WithType
    {
        public string Type { get; }

        public WithType(string? type = null)
        {
            Type = type ?? GetType().Name.ToLowerInvariant();
        }
    }
}
