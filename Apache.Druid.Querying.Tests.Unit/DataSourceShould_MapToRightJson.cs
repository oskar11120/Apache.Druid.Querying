

using Apache.Druid.Querying.DependencyInjection;
using Apache.Druid.Querying.Json;
using Snapshooter.NUnit;

namespace Apache.Druid.Querying.Tests.Unit;
internal class DataSourceShould_MapToRightJson
{
    private static readonly DateTimeOffset t = DateTimeOffset.UnixEpoch.AddYears(30).AddDays(1).AddHours(1).AddMinutes(1);

    private sealed class InlineDataSourceProvider : DataSourceProvider
    {
    }

    private sealed record InlineData(
        [property: DataSourceTimeColumn] DateTimeOffset DataTimeOffset,
        DateTime DateTime,
        bool Boolean,
        double Number);

    [Test]
    public void Inline()
    {
        var provider = new InlineDataSourceProvider();
        (provider as IDataSourceInitializer).Initialize(new(
            DefaultSerializerOptions.Query,
            DefaultSerializerOptions.Data,
            () => throw new NotSupportedException()));

        var query = new Query<InlineData>.Scan();
        var json = provider
            .Inline(new[] { new InlineData(t, t.UtcDateTime, true, 1.5) })
            .MapQueryToJson(query)
            .ToString();
        Snapshot.Match(json);
    }
}
