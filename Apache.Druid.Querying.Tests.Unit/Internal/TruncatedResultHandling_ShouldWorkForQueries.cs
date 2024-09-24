using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Json;
using FluentAssertions;

namespace Apache.Druid.Querying.Tests.Unit.Internal;

internal abstract class Scan_Unordered : Base<int, ScanResult<int>, Query<int>.Scan>
{
    protected Scan_Unordered(Query<int>.Scan query) : base(query)
    {
    }

    protected static QueryResultSimulationAction Return_ToPassOn(int result)
        => Return_ToPassOn(new ScanResult<int>(null, result));
}

internal sealed class Scan_Unordered_WithNoLimit_NoOffset : Scan_Unordered
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

internal sealed class Scan_Unordered_WithNoLimit_ButExistingOffset : Scan_Unordered
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

internal sealed class Scan_Unordered_WithLimit_ButNoOffset : Scan_Unordered
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

internal sealed class Scan_Unordered_WithLimit_AndOffset : Scan_Unordered
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
