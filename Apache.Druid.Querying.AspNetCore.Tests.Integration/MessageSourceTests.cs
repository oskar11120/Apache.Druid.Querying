using Microsoft.Extensions.DependencyInjection;
using System.Globalization;
using static Apache.Druid.Querying.AspNetCore.Tests.Integration.ServiceProvider;

namespace Apache.Druid.Querying.AspNetCore.Tests.Integration;

internal class MessageSourceTests
{
    private static DataSource<Message> Messages => Services.GetRequiredService<DataSource<Message>>();

    [Test]
    public async Task Works()
    {
        var t = DateTime.Parse("2023-10-15T16:57:00.000Z", null, DateTimeStyles.AssumeUniversal).ToUniversalTime();
        var query = new TimeSeriesQuery<Message>
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>()
            .Interval(new(t, t.AddHours(1)))
            .Granularity(Granularity.Minute)
            .Filter(filter => filter.And(
                filter.Equals(
                    message => message.VariableName,
                    "pmPAct"),
                filter.Equals(
                    message => message.TenantId,
                    Guid.Parse("55022f5d-d9c4-4773-86e5-fbce823cd287")),
                filter.Equals(
                    message => message.ObjectId,
                    Guid.Parse("4460391b-b713-44eb-b422-2dbe7de91856"))))
            .Aggregations(aggregations => new[]
            {
                aggregations.Sum(
                    aggregations => aggregations.Sum,
                    message => message.Value),
                aggregations.Count(aggregations => aggregations.Count),
                aggregations.First(
                    aggregations => aggregations.Variable,
                    message => message.VariableName),
                aggregations.First(
                    aggregations => aggregations.FirstValue,
                    message => message.Value)
            });
        try
        {
            var result = await Messages
            .ExecuteQuery(query)
            .ToListAsync();
        }
        catch(Exception ex) 
        {
            throw;
        }
    }

    private sealed record Aggregations(double Sum, int Count, string Variable, double FirstValue);
}
