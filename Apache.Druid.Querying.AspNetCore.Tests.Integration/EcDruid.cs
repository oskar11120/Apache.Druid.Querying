namespace Apache.Druid.Querying.AspNetCore.Tests.Integration
{
    internal sealed class EcDruid : DataSourceProvider
    {
        public DataSource<Message> Variables => Table<Message>("data-variables");
    }
}
