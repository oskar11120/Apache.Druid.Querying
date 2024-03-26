using Apache.Druid.Querying.Internal;
using FluentAssertions;
using Scan = Apache.Druid.Querying.Internal.TruncatedQueryResultHandler<Apache.Druid.Querying.None>
    .Scan<Apache.Druid.Querying.ScanResult<Apache.Druid.Querying.None>>;
using TimeSeries = Apache.Druid.Querying.Internal.TruncatedQueryResultHandler<Apache.Druid.Querying.None>
    .TimeSeries<Apache.Druid.Querying.None>;
using TopN_GroupBy = Apache.Druid.Querying.Internal.TruncatedQueryResultHandler<bool>
    .TopN_GroupBy<bool, bool, Apache.Druid.Querying.Internal.DimensionsProvider<bool>.Identity>;

namespace Apache.Druid.Querying.Tests.Unit.Internal
{
    internal class QueryShould_HandleTruncatedResults
    {
        private static IAsyncEnumerable<T> MaybeTruncate<T>(
            IEnumerable<T> source, bool truncate)
            => source
                .Concat(truncate ? EnumerableEx.Throw<T>(new TruncatedResultsException()) : Enumerable.Empty<T>())
                .ToAsyncEnumerable();

        [Test]
        public async Task Scan()
        {
            var query = new Query<None>.Scan().Limit(1000) as Scan;
            var latest = new Scan.LatestResult();
            var setter = new Mutable<IQueryWithSource<None>>();
            async Task Execute(int start, int count, bool truncate = true)
            {
                var results = Enumerable
                    .Range(start, count)
                    .Select(_ => new ScanResult<None>(null, default));
                (await query
                    .OnTruncatedResultsSetQueryForRemaining(MaybeTruncate(results, truncate), latest, setter, default)
                    .ToListAsync())
                    .Should()
                    .BeEquivalentTo(results);
            }

            await Execute(0, 500);
            latest.Count.Should().Be(500);
            var withOffsetAndLimit = setter.Value as IQueryWith.OffsetAndLimit;
            withOffsetAndLimit.Should().BeEquivalentTo(new { Offset = 500, Limit = 500 });

            await Execute(500, 200);
            latest.Count.Should().Be(700);
            withOffsetAndLimit = setter.Value as IQueryWith.OffsetAndLimit;
            withOffsetAndLimit.Should().BeEquivalentTo(new { Offset = 700, Limit = 300 });

            setter.Value = null;
            await Execute(700, 300, truncate: false);
            latest.Count.Should().Be(1000);
            setter.Value.Should().BeNull();
        }

        [Test]
        public async Task TimeSeries()
        {
            var t0 = DateTime.Today;
            var intervals = new Interval[] { new(t0, t0.AddDays(2)), new(t0.AddDays(3), t0.AddDays(4)) };
            var query = new Query<None>.TimeSeries().Intervals(intervals) as TimeSeries;
            var latest = new TimeSeries.LatestReturned();
            var setter = new Mutable<IQueryWithSource<None>>();
            var deltaT = TimeSpan.FromHours(1);
            async Task Execute(IEnumerable<int> source, bool skipFirstResult = false, bool truncate = true)
            {
                var results = source
                    .Select(i => new WithTimestamp<None>(t0 + i * deltaT, default));
                (await query
                    .OnTruncatedResultsSetQueryForRemaining(MaybeTruncate(results, truncate), latest, setter, default)
                    .ToListAsync())
                    .Should()
                    .BeEquivalentTo(skipFirstResult ? results.Skip(1) : results);
            }

            await Execute(Enumerable.Range(0, 24));
            latest.Timestamp.Should().Be(t0.AddHours(23));
            var withIntervals = setter.Value as IQueryWith.Intervals;
            withIntervals?.Intervals.Should().BeEquivalentTo(
                new Interval[] { new(t0.AddHours(23), t0.AddDays(2)), new(t0.AddDays(3), t0.AddDays(4)) });

            await Execute(Enumerable.Range(23, 25).Concat(Enumerable.Range(72, 12)), skipFirstResult: true);
            latest.Timestamp.Should().Be(t0.AddDays(3).AddHours(11));
            withIntervals = setter.Value as IQueryWith.Intervals;
            withIntervals?.Intervals.Should().BeEquivalentTo(
                new Interval[] { new(t0.AddDays(3).AddHours(11), t0.AddDays(4)) });

            setter.Value = null;
            await Execute(Enumerable.Range(72 + 11, 13), skipFirstResult: true, truncate: false);
            latest.Timestamp.Should().Be(t0.AddDays(4).AddHours(-1));
            setter.Value.Should().BeNull();
        }

        [Test]
        public async Task TopN_GroupBy()
        {
            var t0 = DateTime.Today;
            var latest = new TopN_GroupBy.LatestReturned();
            var setter = new Mutable<IQueryWithSource<bool>>();
            var deltaT = TimeSpan.FromHours(1);
            var query = new Query<bool>.TopN<bool>().Interval(new(t0, t0.AddDays(1))) as TopN_GroupBy;
            async Task Execute(int start, int count, int skipFirstResults = 0, bool truncate = true)
            {
                var results = Enumerable
                    .Range(start, count)
                    .Select(i => new WithTimestamp<bool>(t0 + i / 2 * deltaT, i % 2 is 0));
                (await query
                    .OnTruncatedResultsSetQueryForRemaining(MaybeTruncate(results, truncate), latest, setter, default)
                    .ToListAsync())
                    .Should()
                    .BeEquivalentTo(results.Skip(skipFirstResults));
            }

            await Execute(0, 14 + 13);
            latest.Timestamp.Should().Be(t0.AddHours(13));
            var withIntervals = setter.Value as IQueryWith.Intervals;
            withIntervals?.Intervals.Should().BeEquivalentTo(
                new Interval[] { new(t0.AddHours(13), t0.AddDays(1)) });

            await Execute(14 + 13 - 1, 10, skipFirstResults: 1);
            latest.Timestamp.Should().Be(t0.AddHours(17));
            withIntervals = setter.Value as IQueryWith.Intervals;
            withIntervals?.Intervals.Should().BeEquivalentTo(
                new Interval[] { new(t0.AddHours(17), t0.AddDays(1)) });

            setter.Value = null;
            await Execute(14 + 13 - 1 + 10 - 2, 14, skipFirstResults: 2, truncate: false);
            latest.Timestamp.Should().Be(t0.AddHours(23));
            setter.Value.Should().BeNull();
        }

        [Test]
        public async Task SegmentMetadata()
        {
            static SegmentMetadata New(string id)
                => new(id, default!, default, default, default, default, default, default, default);
            var query = new Query<None>.SegmentMetadata() as TruncatedQueryResultHandler<None>.SegmentMetadata;
            var context = new HashSet<string>();
            var setter = new Mutable<IQueryWithSource<None>>();
            async Task Execute(bool truncate, int[] sourceIds, int[] expectedReturnedIds) 
            {
                var data = MaybeTruncate(sourceIds.Select(id => New(id.ToString())), truncate);
                var results = await query
                    .OnTruncatedResultsSetQueryForRemaining(data, context, setter, CancellationToken.None)
                    .ToListAsync();
                var resultIds = results
                    .Select(result => int.Parse(result.Id));
                resultIds
                    .Should()
                    .BeEquivalentTo(expectedReturnedIds);
                setter.Value.Should().Be(truncate ? query : null);
                setter.Value = null;
            }

            await Execute(true, new[] { 1, 2, 3 }, new[] { 1, 2, 3 });
            await Execute(true, new[] { 3, 1, 2, 5, 4 }, new[] { 5, 4 });
            await Execute(false, new[] { 6, 3, 1, 2, 5, 4 }, new[] { 6 });
        }
    }
}
