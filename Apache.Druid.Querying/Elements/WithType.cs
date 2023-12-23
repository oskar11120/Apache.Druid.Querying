namespace Apache.Druid.Querying.Elements
{
    public abstract class WithType
    {
        public string Type { get; }

        public WithType(string? type = null)
        {
            Type = type ?? GetType().Name.ToLowerInvariant();
        }
    }
}
