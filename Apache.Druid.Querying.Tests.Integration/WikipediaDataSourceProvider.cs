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
        : IEditBooleans, IQueryData<Edit, QueryDataKind.Source>
    {
        [DataSourceColumnSelector(nameof(IsRobot))]
        bool IEditBooleans.Robot { get => IsRobot; }
        Edit IQueryData<Edit, QueryDataKind.Source>.Value => this;
    }

    internal class WikipediaDataSourceProvider : DataSourceProvider
    {
        private static readonly OnMapQueryToJson EnsureOnEditData = (query, json) =>
        {
            if (query is IQueryWith.Source<IOptionalQueryData<Edit, QueryDataKind>> typed) 
            {
                json["isSourceEditVerifiedOnMapToJson"] = true;
            }
        };

        public WikipediaDataSourceProvider()
        {
            Edits = Table<Edit>("wikipedia", EnsureOnEditData);
        }

        public DataSource<Edit> Edits { get; }
    }
}
