namespace Apache.Druid.Querying.Tests.Integration
{
    public interface IEditBooleans
    {
        bool IsNew { get; }
        bool Robot { get; }
    }

    [DataSourceColumnNamingConvention.CamelCase]
    public record Edit(
        [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
        bool IsRobot,
        string Channel,
        string Flags,
        bool IsUnpatrolled,
        string Page,
        [property: DataSourceColumn("diffUrl")] string DiffUri,
        int Added,
        string Comment,
        int CommentLength,
        bool IsNew,
        bool IsMinor,
        int Delta,
        bool IsAnonymous,
        string User,
        int DeltaBucket,
        int Deleted,
        string Namespace,
        string CityName,
        string CountryName,
        string? RegionIsoCode,
        int? MetroCode,
        string? CountryIsoCode,
        string? RegionName)
        : IEditBooleans
    {
        [DataSourceColumnSelector(nameof(IsRobot))]
        bool IEditBooleans.Robot { get => IsRobot; }
    }

    internal class WikipediaDataSourceProvider : DataSourceProvider
    {
        public WikipediaDataSourceProvider()
        {
            Edits = Table<Edit>("wikipedia");
        }

        public DataSource<Edit> Edits { get; }
    }
}
