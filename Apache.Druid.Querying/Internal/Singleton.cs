namespace Apache.Druid.Querying.Internal;

internal static class Singleton<TValue> where TValue : new()
{
    public static readonly TValue Value = new();
}
