namespace Apache.Druid.Querying.Internal.Elements
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
