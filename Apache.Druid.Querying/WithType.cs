namespace Apache.Druid.Querying
{
    public abstract class WithType
    {
        public string Type { get; }

        public WithType(string? type = null)
        {
            Type = type ?? GetType().Name;
        }
    }
}
