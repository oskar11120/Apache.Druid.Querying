namespace Apache.Druid.Querying
{
    internal sealed class DataSource<TItem>
    {
        private readonly string id;

        public DataSource(string id)
        {
            this.id = id;
        }
    }
}
