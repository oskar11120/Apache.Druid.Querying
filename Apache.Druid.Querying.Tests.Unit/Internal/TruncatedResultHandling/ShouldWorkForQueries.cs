using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Json;
using FluentAssertions;
using System.Globalization;

namespace Apache.Druid.Querying.Tests.Unit.Internal.TruncatedResultHandling;

internal abstract class Scan<TResult> : Base<TResult, ScanResult<TResult>, Query<TResult>.Scan>
{
    protected Scan(Query<TResult>.Scan query) : base(query)
    {
    }

    protected static QueryResultSimulationAction Return_ToPassOn(TResult result)
        => Return_ToPassOn(new ScanResult<TResult>(null, result));
    protected static QueryResultSimulationAction Return_ToSkip(TResult result)
        => Return_ToSkip(new ScanResult<TResult>(null, result));
}

internal sealed class Scan_Unordered_WithNoLimit_NoOffset : Scan<int>
{
    public Scan_Unordered_WithNoLimit_NoOffset() : base(new Query<int>.Scan())
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(1);
        yield return Return_ToPassOn(2);
        yield return Return_ToPassOn(3);
        yield return Truncate_ExpectingNextQueryWith.Offset(3);

        yield return Return_ToPassOn(4);
        yield return Return_ToPassOn(5);
        yield return Truncate_ExpectingNextQueryWith.Offset(5);

        yield return Return_ToPassOn(6);
        yield return Return_ToPassOn(7);
    }
}

internal sealed class Scan_Unordered_WithNoLimit_ButExistingOffset : Scan<int>
{
    public Scan_Unordered_WithNoLimit_ButExistingOffset() : base(new Query<int>.Scan().Offset(5))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(1);
        yield return Return_ToPassOn(2);
        yield return Return_ToPassOn(3);
        yield return Truncate_ExpectingNextQueryWith.Offset(3 + 5);

        yield return Return_ToPassOn(4);
        yield return Return_ToPassOn(5);
        yield return Truncate_ExpectingNextQueryWith.Offset(5 + 5);

        yield return Return_ToPassOn(6);
        yield return Return_ToPassOn(7);
    }
}

internal sealed class Scan_Unordered_WithLimit_ButNoOffset : Scan<int>
{
    public Scan_Unordered_WithLimit_ButNoOffset() : base(new Query<int>.Scan().Limit(7))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(1);
        yield return Return_ToPassOn(2);
        yield return Return_ToPassOn(3);
        yield return Truncate_ExpectingNextQueryWith.Limit(7 - 3).Offset(3);

        yield return Return_ToPassOn(4);
        yield return Return_ToPassOn(5);
        yield return Truncate_ExpectingNextQueryWith.Limit(7 - 5).Offset(5);

        yield return Return_ToPassOn(6);
        yield return Return_ToPassOn(7);
    }
}

internal sealed class Scan_Unordered_WithLimit_AndOffset : Scan<int>
{
    public Scan_Unordered_WithLimit_AndOffset() : base(new Query<int>.Scan().Limit(7).Offset(3))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(1);
        yield return Return_ToPassOn(2);
        yield return Return_ToPassOn(3);
        yield return Truncate_ExpectingNextQueryWith.Limit(7 - 3).Offset(3 + 3);

        yield return Return_ToPassOn(4);
        yield return Return_ToPassOn(5);
        yield return Truncate_ExpectingNextQueryWith.Limit(7 - 5).Offset(5 + 3);

        yield return Return_ToPassOn(6);
        yield return Return_ToPassOn(7);
    }
}

internal abstract class Scan_Ordered : Scan<Scan_Ordered.Data>
{
    private static DateTimeOffset Parse(string text) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);
    private static readonly DateTimeOffset t0 = Parse("2024-09-24T15:00:00Z");
    protected static readonly Interval I0 = new(t0, t0.AddHours(1));
    protected static readonly Interval I1 = new(t0.AddHours(3), t0.AddHours(4));

    protected Scan_Ordered(Query<Data>.Scan query) : base(query.Intervals(I0, I1))
    {
    }

    public sealed record Data([property: DataSourceTimeColumn] DateTimeOffset Timestamp, int Value);
}

internal sealed class Scan_Ascending_NoLimit_NoOffset : Scan_Ordered
{
    public Scan_Ascending_NoLimit_NoOffset() : base(new Query<Data>.Scan().Order(OrderDirection.Ascending))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(new(I0.From, 1));
        var t1 = I0.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t1, 1));
        yield return Truncate_ExpectingNextQueryWith.Intervals(I0 with { From = t1 }, I1);

        yield return Return_ToSkip(new(t1, 1));
        yield return Return_ToPassOn(new(t1, 2));
        yield return Return_ToPassOn(new(I1.From, 3));
        var t2 = I1.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t2, 4));
        yield return Truncate_ExpectingNextQueryWith.Interval(I1 with { From = t2 });

        yield return Return_ToSkip(new(t2, 4));
        yield return Return_ToPassOn(new(t2.AddMinutes(1), 5));
    }
}

internal sealed class Scan_Ascending_WithLimit_ButNoOffset : Scan_Ordered
{
    public Scan_Ascending_WithLimit_ButNoOffset() : base(new Query<Data>.Scan().Order(OrderDirection.Ascending).Limit(6).Offset(0))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(new(I0.From, 1));
        var t1 = I0.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t1, 1));
        yield return Truncate_ExpectingNextQueryWith.Intervals(I0 with { From = t1 }, I1).Limit(6 - 2);

        yield return Return_ToSkip(new(t1, 1));
        yield return Return_ToPassOn(new(t1, 2));
        yield return Return_ToPassOn(new(I1.From, 3));
        var t2 = I1.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t2, 4));
        yield return Truncate_ExpectingNextQueryWith.Interval(I1 with { From = t2 }).Limit(6 - 5);

        yield return Return_ToSkip(new(t2, 4));
        yield return Return_ToPassOn(new(t2.AddMinutes(1), 5));
    }
}

internal sealed class Scan_Ascending_WithNoLimit_ButWithOffset : Scan_Ordered
{
    public Scan_Ascending_WithNoLimit_ButWithOffset() : base(new Query<Data>.Scan().Order(OrderDirection.Ascending).Offset(5))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(new(I0.From, 1));
        var t1 = I0.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t1, 1));
        yield return Truncate_ExpectingNextQueryWith.Intervals(I0 with { From = t1 }, I1).Offset(0);

        yield return Return_ToSkip(new(t1, 1));
        yield return Return_ToPassOn(new(t1, 2));
        yield return Return_ToPassOn(new(I1.From, 3));
        var t2 = I1.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t2, 4));
        yield return Truncate_ExpectingNextQueryWith.Interval(I1 with { From = t2 }).Offset(0);

        yield return Return_ToSkip(new(t2, 4));
        yield return Return_ToPassOn(new(t2.AddMinutes(1), 5));
    }
}

internal sealed class Scan_Ascending_WithLimit_AndOffset : Scan_Ordered
{
    public Scan_Ascending_WithLimit_AndOffset() : base(new Query<Data>.Scan().Order(OrderDirection.Ascending).Limit(6).Offset(5))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(new(I0.From, 1));
        var t1 = I0.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t1, 1));
        yield return Truncate_ExpectingNextQueryWith.Intervals(I0 with { From = t1 }, I1).Limit(6 - 2).Offset(0);

        yield return Return_ToSkip(new(t1, 1));
        yield return Return_ToPassOn(new(t1, 2));
        yield return Return_ToPassOn(new(I1.From, 3));
        var t2 = I1.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t2, 4));
        yield return Truncate_ExpectingNextQueryWith.Interval(I1 with { From = t2 }).Limit(6 - 5).Offset(0);

        yield return Return_ToSkip(new(t2, 4));
        yield return Return_ToPassOn(new(t2.AddMinutes(1), 5));
    }
}

internal sealed class Scan_Descending_NoLimit_NoOffset : Scan_Ordered
{
    public Scan_Descending_NoLimit_NoOffset() : base(new Query<Data>.Scan().Order(OrderDirection.Descending))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        var t2 = I1.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t2.AddMinutes(1), 5));
        yield return Return_ToPassOn(new(t2, 4));
        yield return Truncate_ExpectingNextQueryWith.Intervals(I0, I1 with { To = t2.AddMilliseconds(1) });

        yield return Return_ToSkip(new(t2, 4));
        yield return Return_ToPassOn(new(I1.From, 3));
        var t1 = I0.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t1, 2));
        yield return Return_ToPassOn(new(t1, 1));
        yield return Truncate_ExpectingNextQueryWith.Interval(I0 with { To = t1.AddMilliseconds(1) });

        yield return Return_ToSkip(new(t1, 1));
        yield return Return_ToPassOn(new(I0.From, 1));
    }
}

internal sealed class Scan_Descending_WithLimit_AndOffset : Scan_Ordered
{
    public Scan_Descending_WithLimit_AndOffset() : base(new Query<Data>.Scan().Order(OrderDirection.Descending).Limit(6).Offset(5))
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        var t2 = I1.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t2.AddMinutes(1), 5));
        yield return Return_ToPassOn(new(t2, 4));
        yield return Truncate_ExpectingNextQueryWith.Intervals(I0, I1 with { To = t2.AddMilliseconds(1) }).Limit(6 - 2).Offset(0);

        yield return Return_ToSkip(new(t2, 4));
        yield return Return_ToPassOn(new(I1.From, 3));
        var t1 = I0.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t1, 2));
        yield return Return_ToPassOn(new(t1, 1));
        yield return Truncate_ExpectingNextQueryWith.Interval(I0 with { To = t1.AddMilliseconds(1) }).Limit(6 - 5).Offset(0);

        yield return Return_ToSkip(new(t1, 1));
        yield return Return_ToPassOn(new(I0.From, 1));
    }
}

internal abstract class GroupBy_ : Base<GroupBy_.Data, WithTimestamp<int>, Query<GroupBy_.Data>.GroupBy<int>>
{
    private static DateTimeOffset Parse(string text) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);
    private static readonly DateTimeOffset t0 = Parse("2024-09-24T15:00:00Z");
    protected static readonly Interval I0 = new(t0, t0.AddHours(1));
    protected static readonly Interval I1 = new(t0.AddHours(3), t0.AddHours(4));

    protected GroupBy_(Query<Data>.GroupBy<int> query) : base(query.Intervals(I0, I1))
    {
    }

    public sealed record Data([property: DataSourceTimeColumn] DateTimeOffset Timestamp, int Value);
}

internal sealed class GroupBy_Ascending_WithLimit_AndOffset : GroupBy_
{
    public GroupBy_Ascending_WithLimit_AndOffset() : base(new Query<Data>.GroupBy<int>())
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(new(I0.From, 1));
        var t1 = I0.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t1, 1));
        yield return Truncate_ExpectingNextQueryWith.Intervals(I0 with { From = t1 }, I1).Limit(6 - 2).Offset(0);

        yield return Return_ToSkip(new(t1, 1));
        yield return Return_ToPassOn(new(t1, 2));
        yield return Return_ToPassOn(new(I1.From, 3));
        var t2 = I1.From.AddMinutes(1);
        yield return Return_ToPassOn(new(t2, 4));
        yield return Truncate_ExpectingNextQueryWith.Interval(I1 with { From = t2 }).Limit(6 - 5).Offset(0);

        yield return Return_ToSkip(new(t2, 4));
        yield return Return_ToPassOn(new(t2.AddMinutes(1), 5));
    }
}

internal sealed class DataSourceMetadata_ : Base<None, WithTimestamp<DataSourceMetadata>, Query<None>.DataSourceMetadata>
{
    private static DateTimeOffset Parse(string text) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);
    private static readonly DateTimeOffset t0 = Parse("2024-09-24T15:00:00Z");
    private static WithTimestamp<DataSourceMetadata> New(DateTimeOffset t) => new(t, new(t));

    public DataSourceMetadata_() : base(new Query<None>.DataSourceMetadata())
    {
    }

    protected override IEnumerable<QueryResultSimulationAction> SetUpQueryResults()
    {
        yield return Return_ToPassOn(New(t0));
        yield return Return_ToPassOn(New(t0.AddHours(1)));
        yield return Return_ToPassOn(New(t0.AddHours(2)));
        yield return Truncate_ExpectingNextQueryWith;

        yield return Return_ToSkip(New(t0));
        yield return Return_ToSkip(New(t0.AddHours(1)));
        yield return Return_ToSkip(New(t0.AddHours(2)));
        yield return Return_ToPassOn(New(t0.AddHours(3)));
        yield return Return_ToPassOn(New(t0.AddHours(4)));
    }
}

internal abstract class Base<TSource, TResult, TQuery> where TQuery
    : IQueryWith.State, IQueryWith.SourceAndResult<TSource, TResult>
{
    private readonly TQuery query;
    private readonly List<Expectation> expectations = new();
    private int usedExpectations;
    private Expectation GetNextExpatationForAsserting()
    {
        usedExpectations++;
        return expectations[usedExpectations - 1];
    }

    private sealed record Expectation(TQuery? Query, IReadOnlyList<TResult> Results);

    protected Base(TQuery query)
    {
        this.query = query;
    }

    protected sealed class QueryResultSimulationAction
    {
        public TResult? Return_ToPassOn { get; init; }
        public TResult? Return_ToSkip { get; init; }
        public TQuery? Truncate_ExpectingNextQueryToBe { get; init; }

        public static implicit operator QueryResultSimulationAction(TQuery queryAfterTruncate)
            => new() { Truncate_ExpectingNextQueryToBe = queryAfterTruncate };
    }

    protected static QueryResultSimulationAction Return_ToPassOn(TResult result)
        => new() { Return_ToPassOn = result };
    protected static QueryResultSimulationAction Return_ToSkip(TResult result)
        => new() { Return_ToSkip = result };
    protected TQuery Truncate_ExpectingNextQueryWith => query.Copy();
    protected abstract IEnumerable<QueryResultSimulationAction> SetUpQueryResults();

    [Test]
    public async Task QueryShouldHandleTruncatedResults()
    {
        var context = new TruncatedResultHandlingContext<TSource>(PropertyColumnNameMapping.ImmutableBuilder.Create<TSource>());
        var queryResultSets = GetQueryResults_SetExpectations();
        foreach (var queryResults in queryResultSets)
        {
            context.NextQuerySetter = null;
            var handlerResults = await query
                .OnTruncatedResultsSetQueryForRemaining(queryResults, context, default)
                .ToListAsync();
            var expected = GetNextExpatationForAsserting();

            var resultQueryJson = context.NextQuerySetter?.MapToJson().ToString();
            var expectedQueryJson = expected.Query?.MapToJson().ToString();
            TestContext.Out.WriteLine(nameof(resultQueryJson));
            TestContext.Out.Write(resultQueryJson);
            TestContext.Out.WriteLine();
            TestContext.Out.WriteLine(nameof(expectedQueryJson));
            TestContext.Out.Write(expectedQueryJson);
            TestContext.Out.WriteLine();
            TestContext.Out.WriteLine();
            resultQueryJson.Should().Be(expectedQueryJson);

            handlerResults.Should().BeEquivalentTo(expected.Results, options => options.WithStrictOrdering());
        }
    }

    private IEnumerable<IAsyncEnumerable<TResult>> GetQueryResults_SetExpectations()
    {
        var finished = false;
        using var resultIterator = SetUpQueryResults().GetEnumerator();
        List<TResult> expectedResults = new();
        async IAsyncEnumerable<TResult> GetNext()
        {
            await Task.CompletedTask;
            while (resultIterator.MoveNext())
            {
                var current = resultIterator.Current;
                if (current.Return_ToPassOn is TResult toPassOn)
                {
                    expectedResults.Add(toPassOn);
                    yield return toPassOn;
                }
                else if (current.Return_ToSkip is TResult toSkip)
                    yield return toSkip;
                else
                {
                    expectations.Add(new(current.Truncate_ExpectingNextQueryToBe!, expectedResults));
                    expectedResults = new();
                    throw new UnexpectedEndOfJsonStreamException();
                }
            }

            finished = true;
            expectations.Add(new(default, expectedResults));
        }

        while (!finished)
            yield return GetNext();
    }
}
