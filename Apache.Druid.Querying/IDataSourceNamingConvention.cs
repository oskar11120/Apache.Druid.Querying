namespace Apache.Druid.Querying
{
    public interface IDataSourceNamingConvention
    {
        string Apply(string memberName);
    }
}
