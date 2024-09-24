using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Json;
using FluentAssertions;
using Scan = Apache.Druid.Querying.Internal.TruncatedQueryResultHandler<Apache.Druid.Querying.None>
    .Scan<Apache.Druid.Querying.None>;
using TimeSeries = Apache.Druid.Querying.Internal.TruncatedQueryResultHandler<Apache.Druid.Querying.None>
    .TimeSeries<Apache.Druid.Querying.None>;
using TopN_GroupBy = Apache.Druid.Querying.Internal.TruncatedQueryResultHandler<bool>
    .GroupBy<bool, bool>;
using Segment_Metadata = Apache.Druid.Querying.Internal.TruncatedQueryResultHandler<Apache.Druid.Querying.None>.SegmentMetadata;

namespace Apache.Druid.Querying.Tests.Unit.Internal
{
    internal class QueryShould_HandleTruncatedResults
    {
        private static IAsyncEnumerable<T> MaybeTruncate<T>(
            IEnumerable<T> source, bool truncate)
            => source
                .Concat(truncate ? EnumerableEx.Throw<T>(new UnexpectedEndOfJsonStreamException()) : Enumerable.Empty<T>())
                .ToAsyncEnumerable();

        [Test]
        public async Task Scan_Unordered()
        {
            var query = new Query<None>.Scan().Limit(1000) as Scan;
            var state = new Scan.TruncatedResultHandlingContextState();
            int Count() => state.GetOrAdd<Scan.UnorderedQueryHandlingState>().ReturnCount;
            var mapping = PropertyColumnNameMapping.ImmutableBuilder.Create<None>();
            IQueryWith.Source<None>? nextQuery = null;
            async Task Execute(int start, int count, bool truncate = true)
            {
                var results = Enumerable
                    .Range(start, count)
                    .Select(_ => new ScanResult<None>(null, None.Singleton));
                var context = new Scan.TruncatedResultHandlingContext(mapping, MaybeTruncate(results, truncate), state);
                (await query
                    .OnTruncatedResultsSetQueryForRemaining(context, default)
                    .ToListAsync())
                    .Should()
                    .BeEquivalentTo(results);
                nextQuery = context.NextQuerySetter;
            }

            await Execute(0, 500);
            Count().Should().Be(500);
            var withOffsetAndLimit = nextQuery as IQueryWith.OffsetAndLimit;
            void ShouldBeEquivalentTo(int offset, int limit)
            {
                withOffsetAndLimit!.Offset.Should().Be(offset);
                withOffsetAndLimit.Limit.Should().Be(limit);
            }
            ShouldBeEquivalentTo(offset: 500, limit: 500);

            await Execute(500, 200);
            Count().Should().Be(700);
            withOffsetAndLimit = nextQuery as IQueryWith.OffsetAndLimit;
            ShouldBeEquivalentTo(offset: 700, limit: 300);

            nextQuery = null;
            await Execute(700, 300, truncate: false);
            Count().Should().Be(1000);
            nextQuery.Should().BeNull();
        }

        [Test]
        public async Task TimeSeries()
        {
            var t0 = (DateTimeOffset)DateTime.Today;
            var intervals = new Interval[] { new(t0, t0.AddDays(2)), new(t0.AddDays(3), t0.AddDays(4)) };
            var query = new Query<None>.TimeSeries().Intervals(intervals) as TimeSeries;
            var state = new TimeSeries.TruncatedResultHandlingContextState();
            var latest = state.GetOrAdd<TruncatedQueryResultHandler<None>.GivenOrdered_MultiplePerTimestamp_Results.LatestReturned<None>>();
            var deltaT = TimeSpan.FromHours(1);
            IQueryWith.Source<None>? nextQuery = null;
            async Task Execute(IEnumerable<int> source, bool skipFirstResult = false, bool truncate = true)
            {
                var results = source
                    .Select(i => new WithTimestamp<None>(t0 + i * deltaT, None.Singleton))
                    .ToArray();
                var context = new TimeSeries.TruncatedResultHandlingContext(null!, MaybeTruncate(results, truncate), state);
                var returned = await query
                    .OnTruncatedResultsSetQueryForRemaining(context, default)
                    .ToListAsync();
                returned
                    .Should()
                    .BeEquivalentTo(skipFirstResult ? results.Skip(1) : results);
                nextQuery = context.NextQuerySetter;
            }

            await Execute(Enumerable.Range(0, 24));
            latest.Timestamp.Should().Be(t0.AddHours(23));
            var withIntervals = nextQuery as IQueryWith.Intervals;
            withIntervals?.Intervals.Should().BeEquivalentTo(
                new Interval[] { new(t0.AddHours(23), t0.AddDays(2)), new(t0.AddDays(3), t0.AddDays(4)) });

            await Execute(Enumerable.Range(23, 25).Concat(Enumerable.Range(72, 12)), skipFirstResult: true);
            latest.Timestamp.Should().Be(t0.AddDays(3).AddHours(11));
            withIntervals = nextQuery as IQueryWith.Intervals;
            withIntervals?.Intervals.Should().BeEquivalentTo(
                new Interval[] { new(t0.AddDays(3).AddHours(11), t0.AddDays(4)) });

            nextQuery = null;
            await Execute(Enumerable.Range(72 + 11, 13), skipFirstResult: true, truncate: false);
            latest.Timestamp.Should().Be(t0.AddDays(4).AddHours(-1));
            nextQuery.Should().BeNull();
        }

        [Test]
        public async Task TopN_GroupBy()
        {
            var t0 = DateTime.Today;
            var state = new TopN_GroupBy.TruncatedResultHandlingContextState();
            var latest = state.GetOrAdd<TruncatedQueryResultHandler<bool>.GivenOrdered_MultiplePerTimestamp_Results.LatestReturned<bool>>();
            var deltaT = TimeSpan.FromHours(1);
            var query = new Query<bool>.GroupBy<bool>().Interval(new(t0, t0.AddDays(1))) as TopN_GroupBy;
            IQueryWith.Source<bool>? nextQuery = null;
            async Task Execute(int start, int count, int skipFirstResults = 0, bool truncate = true)
            {
                var results = Enumerable
                    .Range(start, count)
                    .Select(i => new WithTimestamp<bool>(t0 + i / 2 * deltaT, i % 2 is 0));
                var context = new TopN_GroupBy.TruncatedResultHandlingContext(null!, MaybeTruncate(results, truncate), state);
                (await query
                    .OnTruncatedResultsSetQueryForRemaining(context, default)
                    .ToListAsync())
                    .Should()
                    .BeEquivalentTo(results.Skip(skipFirstResults));
                nextQuery = context.NextQuerySetter;
            }

            await Execute(0, 14 + 13);
            latest.Timestamp.Should().Be(t0.AddHours(13));
            var withIntervals = nextQuery as IQueryWith.Intervals;
            withIntervals?.Intervals.Should().BeEquivalentTo(
                new Interval[] { new(t0.AddHours(13), t0.AddDays(1)) });

            await Execute(14 + 13 - 1, 10, skipFirstResults: 1);
            latest.Timestamp.Should().Be(t0.AddHours(17));
            withIntervals = nextQuery as IQueryWith.Intervals;
            withIntervals?.Intervals.Should().BeEquivalentTo(
                new Interval[] { new(t0.AddHours(17), t0.AddDays(1)) });

            nextQuery = null;
            await Execute(14 + 13 - 1 + 10 - 2, 14, skipFirstResults: 2, truncate: false);
            latest.Timestamp.Should().Be(t0.AddHours(23));
            nextQuery.Should().BeNull();
        }

        [Test]
        public async Task SegmentMetadata()
        {
            static SegmentMetadata New(string id)
                => new(id, default!, default, default, default, default, default, default, default);
            var query = new Query<None>.SegmentMetadata() as Segment_Metadata;
            var state = new Segment_Metadata.TruncatedResultHandlingContextState();
            async Task Execute(bool truncate, int[] sourceIds, int[] expectedReturnedIds)
            {
                var data = MaybeTruncate(sourceIds.Select(id => New(id.ToString())), truncate);
                var context = new Segment_Metadata.TruncatedResultHandlingContext(null!, data, state);
                var results = await query
                    .OnTruncatedResultsSetQueryForRemaining(context, CancellationToken.None)
                    .ToListAsync();
                var resultIds = results
                    .Select(result => int.Parse(result.Id));
                resultIds
                    .Should()
                    .BeEquivalentTo(expectedReturnedIds);
                context.NextQuerySetter.Should().Be(truncate ? query : null);
                context.NextQuerySetter = null;
            }

            await Execute(true, new[] { 1, 2, 3 }, new[] { 1, 2, 3 });
            await Execute(true, new[] { 3, 1, 2, 5, 4 }, new[] { 5, 4 });
            await Execute(false, new[] { 6, 3, 1, 2, 5, 4 }, new[] { 6 });
        }
    }
}
