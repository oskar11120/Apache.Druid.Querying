namespace Apache.Druid.Querying.Tests.Integration
{
    internal sealed class EcDruid : DataSourceProvider
    {
        public DataSource<VariableMessage> Variables => Table<VariableMessage>("data-variables");
        public DataSource<EventMessage> Events => Table<EventMessage>("data-events");
    }
}
