using MoreLinq;
using Snapshooter.NUnit;
using System.Globalization;
using static Apache.Druid.Querying.Tests.Integration.ServiceProvider;

namespace Apache.Druid.Querying.Tests.Integration;

internal class PeriodAndSimpleGranularitiesShould
{
    public static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2016-06-27T00:00:00.000Z", CultureInfo.InvariantCulture);
    private const string utc = "Utc";

    private static readonly object[] testCases = new[] { utc, "Europe/Warsaw" }
        .Cartesian(
            Enum.GetValues<Granularity>().Where(granularity => granularity is > Granularity.Hour),
            (timeZone, granularity) => new object[] { granularity, timeZone })
        .ToArray();

    private readonly record struct Aggregations(int? Sum);
    [TestCaseSource(nameof(testCases))]
    public async Task BeConsistent(Granularity granularity, string timeZone)
    {
        var snapshotName = Snapshot.FullName();
        snapshotName = new(snapshotName.Filename.Replace("/", ""), snapshotName.FolderPath);
        var query = new Query<Edit>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>()
            .Aggregations(type => new(type.Sum(edit => edit.Added)))
            .Interval(new(T0.AddDays(-10), T0.AddDays(10)))
            .Granularity(granularity, timeZone);
        var results = await Wikipedia.Edits.ExecuteQuery(query).ToListAsync();
        Snapshot.Match(results, snapshotName);

        if (timeZone != utc)
            return;

        query.Granularity(granularity);
        results = await Wikipedia.Edits.ExecuteQuery(query).ToListAsync();
        Snapshot.Match(results, snapshotName);
    }
}
