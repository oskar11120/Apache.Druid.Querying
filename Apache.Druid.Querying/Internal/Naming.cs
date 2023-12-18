namespace Apache.Druid.Querying.Internal
{
    internal static class Naming
    {
        public static string ToCamelCase(this string pascalCase) => string.Create(
            pascalCase.Length,
            pascalCase,
            (span, pascalCase) =>
            {
                pascalCase.CopyTo(span);
                span[0] = char.ToLowerInvariant(pascalCase[0]);
            });
    }
}
