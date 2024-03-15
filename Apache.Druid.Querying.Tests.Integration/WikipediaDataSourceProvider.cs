using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

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
        [JsonInclude]
        [SuppressMessage("CodeQuality", "IDE0051:Remove unused private members", Justification = "For deseirialization.")]
        private bool Robot { init => IsRobot = value; }

        [DataSourceColumn("isRobot")]
        bool IEditBooleans.Robot { get => IsRobot; }
    }

    internal sealed class WikipediaDataSourceProvider : DataSourceProvider
    {
        public WikipediaDataSourceProvider()
        {
            Edits = Table<Edit>("wikipedia");
        }

        public DataSource<Edit> Edits { get; }
    }
}
