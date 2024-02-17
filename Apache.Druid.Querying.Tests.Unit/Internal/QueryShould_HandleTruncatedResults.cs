using FluentAssertions;
using System.Linq;
using Scan = Apache.Druid.Querying.Internal.TruncatedQueryResultHandler<int>.Scan<Apache.Druid.Querying.ScanResult<int>>;

namespace Apache.Druid.Querying.Tests.Unit.Internal
{
    internal class QueryShould_HandleTruncatedResults
    {
        [Test]
        public async Task Scan()
        {
            var query = new Query<int>.Scan().Limit(1000) as Scan;
            var latest = new Scan.LatestResult();
            var setter = new Mutable<IQueryWithSource<int>>();
            async Task Execute(int start, int count, bool truncated = true)
            {
                var range = Enumerable
                    .Range(start, count)
                    .Select(i => new ScanResult<int>(null, i));
                var maybeTruncated = range
                    .ToAsyncEnumerable()
                    .Concat(truncated ?
                        AsyncEnumerableEx.Throw<ScanResult<int>>(new TruncatedResultsException()) :
                        AsyncEnumerable.Empty<ScanResult<int>>());
                (await query
                    .OnTruncatedResultsSetQueryForRemaining(maybeTruncated, latest, setter, default)
                    .ToListAsync())
                    .Should()
                    .BeEquivalentTo(range);
            }

            await Execute(0, 500);
            latest.Count.Should().Be(500);
            var offsetAndLimit = setter.Value as IQueryWith.OffsetAndLimit;
            offsetAndLimit.Should().BeEquivalentTo(new { Offset = 500, Limit = 500 });

            await Execute(500, 200);
            latest.Count.Should().Be(700);
            offsetAndLimit = setter.Value as IQueryWith.OffsetAndLimit;
            offsetAndLimit.Should().BeEquivalentTo(new { Offset = 700, Limit = 300 });

            setter.Value = null;
            await Execute(700, 300, truncated: false);
            latest.Count.Should().Be(1000);
            setter.Value.Should().BeNull();
        }
    }
}
