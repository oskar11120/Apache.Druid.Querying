using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Apache.Druid.Querying.Internal;

public interface IDimensionsProvider<in TResult, out TDimensions>
{
    internal TDimensions GetDimensions(TResult result);
}

public static class DimensionsProvider<TDimensions>
{
    public sealed class Identity : IDimensionsProvider<TDimensions, TDimensions>
    {
        TDimensions IDimensionsProvider<TDimensions, TDimensions>.GetDimensions(TDimensions result) => result;
    }

    public sealed class FromResult<TResult>
        : IDimensionsProvider<TResult, TDimensions>
        where TResult : IQueryDataWithDimensions<TDimensions>
    {
        TDimensions IDimensionsProvider<TResult, TDimensions>.GetDimensions(TResult result) => result.Dimensions;
    }
}

public static class TruncatedQueryResultHandler<TSource>
{
    private static TQuery WithIntervalsStartingFrom<TQuery>(TQuery query, DateTimeOffset from)
        where TQuery : IQueryWith.Intervals
    {
        var newIntervals = query
            .Intervals
            .Select(interval =>
            {
                if (interval.To <= from)
                    return null;
                return interval.From <= from && interval.To > from ?
                    interval with { From = from } : interval;
            })
            .Where(inteval => inteval is not null)
            .ToArray();
        var result = query.Copy().Intervals(newIntervals!);
        return result;
    }

    public interface TimeSeries<TResult> :
        IQueryWithSource<TSource>.AndResult<WithTimestamp<TResult>>.AndDeserializationAndTruncatedResultHandling<TimeSeries<TResult>.LatestReturned>,
        IQueryWith.Intervals,
        ICloneableQuery<IQueryWithSource<TSource>>
    {
        public sealed class LatestReturned
        {
            public DateTimeOffset? Timestamp;
        }

        async IAsyncEnumerable<WithTimestamp<TResult>> AndDeserializationAndTruncatedResultHandling<LatestReturned>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> results,
            LatestReturned latestReturned,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = results.Catch<WithTimestamp<TResult>, TruncatedResultsException>(_ => truncated = true, token);
            await foreach (var result in results)
            {
                if (latestReturned.Timestamp != result.Timestamp)
                {
                    latestReturned.Timestamp = result.Timestamp;
                    yield return result;
                }
            }

            if (!truncated)
                yield break;
            setter.Value = latestReturned.Timestamp is DateTimeOffset existing ? WithIntervalsStartingFrom(this, existing) : this;
        }
    }

    public interface TopN_GroupBy<TResult, TDimensions, TDimensionsProvider> :
        IQueryWithSource<TSource>.AndResult<WithTimestamp<TResult>>.AndDeserializationAndTruncatedResultHandling<TopN_GroupBy<TResult, TDimensions, TDimensionsProvider>.LatestReturned>,
        IQueryWith.Intervals
        where TDimensions : IEquatable<TDimensions>
        where TDimensionsProvider : IDimensionsProvider<TResult, TDimensions>, new()
    {
        public sealed class LatestReturned
        {
            public DateTimeOffset? Timestamp;
            public Queue<TDimensions> Dimensions = new();
        }

        private static readonly TDimensionsProvider provider = new();

        async IAsyncEnumerable<WithTimestamp<TResult>> AndDeserializationAndTruncatedResultHandling<LatestReturned>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> results,
            LatestReturned latestReturned,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = results.Catch<WithTimestamp<TResult>, TruncatedResultsException>(_ => truncated = true, token);
            var timestampChangedAtLeastOnce = false;
            await foreach (var result in results)
            {
                var timestampChanged = latestReturned.Timestamp != result.Timestamp;
                timestampChangedAtLeastOnce = timestampChangedAtLeastOnce || timestampChanged;
                if (timestampChanged)
                {
                    latestReturned.Timestamp = result.Timestamp;
                    latestReturned.Dimensions.Clear();
                }

                var resultDimensions = provider.GetDimensions(result.Value);
                if (timestampChangedAtLeastOnce || !latestReturned.Dimensions.Contains(resultDimensions))
                {
                    latestReturned.Dimensions.Enqueue(resultDimensions);
                    yield return result;
                }
            }

            if (!truncated)
                yield break;
            setter.Value = latestReturned.Timestamp is DateTimeOffset existing ? WithIntervalsStartingFrom(this, existing) : this;
        }
    }

    // TODO Try to optimize by using intervals if query is ordered.
    public interface Scan<TResult> :
        IQueryWithSource<TSource>.AndResult<TResult>.AndDeserializationAndTruncatedResultHandling<Scan<TResult>.LatestResult>,
        IQueryWith.OffsetAndLimit
    {
        public sealed class LatestResult
        {
            public int Count;
        }

        async IAsyncEnumerable<TResult> AndDeserializationAndTruncatedResultHandling<LatestResult>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TResult> results,
            LatestResult latestResult,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = results.Catch<TResult, TruncatedResultsException>(_ => truncated = true, token);
            await foreach (var result in results)
            {
                latestResult.Count++;
                yield return result;
            }

            if (!truncated || (Limit is not null && latestResult.Count >= Limit))
                yield break;
            var newQuery = this.Copy().Offset(latestResult.Count);
            if (Limit is not null)
                newQuery = newQuery.Limit(Limit.Value - latestResult.Count);
            setter.Value = newQuery;
        }
    }

    public interface SegmentMetadata :
        IQueryWithSource<TSource>.AndResult<Querying.SegmentMetadata>.AndDeserializationAndTruncatedResultHandling<HashSet<string>>
    {
        async IAsyncEnumerable<Querying.SegmentMetadata> AndDeserializationAndTruncatedResultHandling<HashSet<string>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<Querying.SegmentMetadata> results,
            HashSet<string> returnedSegmentIds,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = results.Catch<Querying.SegmentMetadata, TruncatedResultsException>(_ => truncated = true, token);
            await foreach (var result in results)
                if (returnedSegmentIds.Add(result.Id))
                    yield return result;

            if (truncated)
                setter.Value = this;
        }
    }
}
