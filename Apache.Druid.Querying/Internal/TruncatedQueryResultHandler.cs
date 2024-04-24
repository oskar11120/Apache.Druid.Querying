using Apache.Druid.Querying.Json;
using System;
using System.Collections.Generic;
using System.IO;
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
    private static async IAsyncEnumerable<TItem> OnTruncatedResultsInvoke<TItem>(
        IAsyncEnumerable<TItem> results, Action action, [EnumeratorCancellation] CancellationToken token)
    {
        var enumerator = results.GetAsyncEnumerator(token);
        while (true)
        {
            TItem item;
            try
            {
                if (!await enumerator.MoveNextAsync())
                    break;
                item = enumerator.Current;
            }
            catch (IOException io) when (io.Message.StartsWith("The response ended prematurely", StringComparison.Ordinal))
            {
                action();
                break;
            }
            catch (UnexpectedEndOfJsonStreamException)
            {
                action();
                break;
            }

            yield return item;
        }
    }

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

    public interface Base<TResult, TContext> : IQueryWithSource<TSource>.AndResult<TResult>
        where TContext : new()
    {
        IAsyncEnumerable<TResult> AndResult<TResult>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TResult> results,
            TruncatedQueryResultHandlingContext context,
            Mutable<IQueryWithSource<TSource>> setter,
            CancellationToken token)
        {
            context.State ??= new TContext();
            return OnTruncatedResultsSetQueryForRemaining(results, (TContext)context.State, setter, token);
        }

        internal IAsyncEnumerable<TResult> OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TResult> results, TContext context, Mutable<IQueryWithSource<TSource>> setter, CancellationToken token);
    }

    public interface TimeSeries<TResult> :
        Base<WithTimestamp<TResult>, TimeSeries<TResult>.LatestReturned>,
        IQueryWith.Intervals,
        ICloneableQuery<IQueryWithSource<TSource>>
    {
        public sealed class LatestReturned
        {
            public DateTimeOffset? Timestamp;
        }

        async IAsyncEnumerable<WithTimestamp<TResult>> Base<WithTimestamp<TResult>, TimeSeries<TResult>.LatestReturned>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> results,
            LatestReturned latestReturned,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = OnTruncatedResultsInvoke(results, () => truncated = true, token);
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
        Base<WithTimestamp<TResult>, TopN_GroupBy<TResult, TDimensions, TDimensionsProvider>.LatestReturned>,
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

        async IAsyncEnumerable<WithTimestamp<TResult>> Base<WithTimestamp<TResult>, LatestReturned>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> results,
            LatestReturned latestReturned,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = OnTruncatedResultsInvoke(results, () => truncated = true, token);
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
        Base<TResult, Scan<TResult>.LatestResult>,
        IQueryWith.OffsetAndLimit
    {
        public sealed class LatestResult
        {
            public int Count;
        }

        async IAsyncEnumerable<TResult> Base<TResult, Scan<TResult>.LatestResult>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TResult> results,
            LatestResult latestResult,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = OnTruncatedResultsInvoke(results, () => truncated = true, token);
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

    public interface SegmentMetadata : Base<Querying.SegmentMetadata, HashSet<string>>
    {
        async IAsyncEnumerable<Querying.SegmentMetadata> Base<Querying.SegmentMetadata, HashSet<string>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<Querying.SegmentMetadata> results,
            HashSet<string> returnedSegmentIds,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = OnTruncatedResultsInvoke(results, () => truncated = true, token);
            await foreach (var result in results)
                if (returnedSegmentIds.Add(result.Id))
                    yield return result;

            if (truncated)
                setter.Value = this;
        }
    }
}
