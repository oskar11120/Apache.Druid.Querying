using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Apache.Druid.Querying.Internal;

internal interface IWithDimensions<TDimensions>
{
    internal TDimensions Dimensions { get; }
}

internal static class TruncatedQueryResultHandler<TSource>
{
    private class Copy_<TQuery> : IQueryWithSource<TSource>, IQueryWith.Intervals
        where TQuery : IQueryWithSource<TSource>, IQueryWith.Intervals
    {
        public Copy_(TQuery @base)
        {
            State = @base.GetState().ToDictionary(pair => pair.Key, pair => pair.Value);
            SectionAtomicity = @base.SectionAtomicity;
            Intervals = @base.Intervals;
        }

        public Dictionary<string, QuerySectionValueFactory> State { get; }
        public SectionAtomicity.IProvider.Builder SectionAtomicity { get; }
        public IReadOnlyCollection<Interval>? Intervals { get; set; }
    }

    private static Copy_<TQuery> Copy<TQuery>(TQuery query)
        where TQuery : IQueryWithSource<TSource>, IQueryWith.Intervals
        => new(query);

    private static Copy_<TQuery> Copy<TQuery>(TQuery query, DateTimeOffset withIntervalsStartingFrom)
        where TQuery : IQueryWithSource<TSource>, IQueryWith.Intervals
    {
        var newIntervals = query.
            GetIntervals()
            .Select(interval => interval.From <= withIntervalsStartingFrom && interval.To < withIntervalsStartingFrom ?
                interval with { From = withIntervalsStartingFrom } : interval)
            .ToArray();
        return Copy(query).Intervals(newIntervals);
    }

    public interface TimeSeries<TResult> :
        IQueryWithSource<TSource>.AndResult<WithTimestamp<TResult>>.AndDeserializationAndTruncatedResultHandling<TimeSeries<TResult>.LatestReturned>,
        IQueryWith.Intervals
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
                if (latestReturned.Timestamp is null || result.Timestamp > latestReturned.Timestamp)
                {
                    latestReturned.Timestamp = result.Timestamp;
                    yield return result;
                }
            }

            if (!truncated)
                yield break;
            setter.Value = latestReturned.Timestamp is DateTimeOffset existing ? Copy(this, existing) : this;
        }
    }

    public interface TopN_GroupBy<TResult, TDimensions> :
        IQueryWithSource<TSource>.AndResult<WithTimestamp<TResult>>.AndDeserializationAndTruncatedResultHandling<TopN_GroupBy<TResult, TDimensions>.LatestReturned>,
        IQueryWith.Intervals
        where TDimensions : IEquatable<TDimensions>
        where TResult : IWithDimensions<TDimensions>
    {
        public sealed class LatestReturned
        {
            public DateTimeOffset? Timestamp;
            public Queue<TDimensions> Dimensions = new();
        }

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
                var timestampChanged = latestReturned.Timestamp is null || result.Timestamp > latestReturned.Timestamp;
                timestampChangedAtLeastOnce = timestampChangedAtLeastOnce || timestampChanged;
                if (timestampChanged)
                {
                    latestReturned.Timestamp = result.Timestamp;
                    latestReturned.Dimensions.Clear();
                }

                var resultDimensions = result.Value.Dimensions;
                if (timestampChangedAtLeastOnce || result.Timestamp == latestReturned.Timestamp && !latestReturned.Dimensions.Contains(resultDimensions))
                {
                    latestReturned.Dimensions.Enqueue(resultDimensions);
                    yield return result;
                }
            }

            if (!truncated)
                yield break;
            setter.Value = latestReturned.Timestamp is DateTimeOffset existing ? Copy(this, existing) : this;
        }
    }

    // TODO Try to optimize by using intervals if query is ordered.
    public interface Scan<TResult> :
        IQueryWithSource<TSource>.AndResult<WithTimestamp<TResult>>.AndDeserializationAndTruncatedResultHandling<Scan<TResult>.LatestResult>,
        IQueryWith.Intervals,
        IQueryWith.OffsetAndLimit
    {
        private sealed class Copy__<TQuery> : Copy_<TQuery>, IQueryWith.OffsetAndLimit where TQuery :
            IQueryWithSource<TSource>,
            IQueryWith.Intervals,
            IQueryWith.OffsetAndLimit
        {
            public Copy__(TQuery @base) : base(@base)
            {
                Offset = @base.Offset;
                Limit = @base.Limit;
            }

            public int Offset { get; set; }
            public int Limit { get; set; }
        }

        private static Copy__<TQuery> Copy<TQuery>(TQuery query) where TQuery :
            IQueryWithSource<TSource>,
            IQueryWith.Intervals,
            IQueryWith.OffsetAndLimit
             => new(query);

        public sealed class LatestResult
        {
            public int Count;
        }

        async IAsyncEnumerable<WithTimestamp<TResult>> AndDeserializationAndTruncatedResultHandling<LatestResult>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> results,
            LatestResult latestResult,
            Mutable<IQueryWithSource<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = results.Catch<WithTimestamp<TResult>, TruncatedResultsException>(_ => truncated = true, token);
            await foreach (var result in results)
            {
                latestResult.Count++;
                yield return result;
            }

            if (!truncated || latestResult.Count >= Limit)
                yield break;
            setter.Value = Copy(this).Offset(latestResult.Count).Limit(Limit - latestResult.Count);
        }
    }
}
