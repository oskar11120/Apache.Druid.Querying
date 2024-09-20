namespace Apache.Druid.Querying.Internal.Elements
{
    internal abstract class WithType
    {
        public string Type { get; }

        public WithType(string? type = null)
        {
            if (type is null)
            {
                var clrType = GetType();
                type = clrType.Name.ToLowerInvariant();
                if (type.IndexOf('`') is not -1 and var index)
                    type = type[..index];
            }

            Type = type;
        }
    }
}
