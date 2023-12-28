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
        var t = DateTime.Parse("2023-10-10T16:57:00.000Z", null, DateTimeStyles.AssumeUniversal).ToUniversalTime();
        var query = new Query<Message>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>
            .WithPostAggregations<PostAggregations>()
            .Interval(new(t, t.AddDays(10)))
            .Granularity(Granularity.Day)
            .Filter(filter => filter.And(
                filter.Selector(
                    message => message.VariableName,
                    "pmPAct"),
                filter.Selector(
                    message => message.TenantId,
                    Guid.Parse("55022f5d-d9c4-4773-86e5-fbce823cd287")),
                filter.Selector(
                    message => message.ObjectId,
                    Guid.Parse("4460391b-b713-44eb-b422-2dbe7de91856"))))
            .Aggregations(aggregators => new(
                aggregators.Sum(message => message.Value),
                aggregators.Count(),
                aggregators.First(message => message.VariableName),
                aggregators.First(message => message.Value, SimpleDataType.String)
            ))
            //.PostAggregations(postAggregators => new[]
            //{
            //    postAggregators.Arithmetic(
            //        postAggregations => postAggregations.Average,
            //        ArithmeticFunction.Divide,
            //        postAggregators.FieldAccess(aggrgations => aggrgations.Sum),
            //        postAggregators.FieldAccess(aggregations => aggregations.Count))
            //})
            .Context(new() { SkipEmptyBuckets = true });


        try
        {
            var result = await Messages
                .ExecuteQuery(query)
                .ToListAsync();
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    private sealed record Aggregations(double Sum, int Count, string Variable, double? FirstValue);
    private sealed record PostAggregations(double Average);
}
