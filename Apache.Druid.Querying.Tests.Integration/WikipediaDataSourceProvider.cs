namespace Apache.Druid.Querying.Tests.Integration
{
    [DataSourceColumnNamingConvention(DataSourceColumnNamingConventionType.CamelCase)]
    public record Edit(
        DateTimeOffset? Timestamp,
        bool IsRobot,
        string Channel,
        string Flags,
        bool IsUnpatroled,
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
        string? RegionName);

    internal sealed class WikipediaDataSourceProvider : DataSourceProvider
    {
        public WikipediaDataSourceProvider()
        {
            Edits = Table<Edit>("wikipedia");
        }

        public DataSource<Edit> Edits { get; }
    }
}
