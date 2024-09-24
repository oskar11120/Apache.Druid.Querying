using Apache.Druid.Querying.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Apache.Druid.Querying.Internal;

public interface IDimensionsProvider<in TResult, out TDimensions>
{
    internal TDimensions GetDimensions(TResult result);
}

public static class DimensionsProvider<TDimensions>
{
    public interface Identity : IDimensionsProvider<TDimensions, TDimensions>
    {
        TDimensions IDimensionsProvider<TDimensions, TDimensions>.GetDimensions(TDimensions result) => result;
    }

    public interface FromResult<TResult>
        : IDimensionsProvider<TResult, TDimensions>
        where TResult : IQueryData<TDimensions, QueryDataKind.Dimensions>
    {
        TDimensions IDimensionsProvider<TResult, TDimensions>.GetDimensions(TResult result) => result.Value;
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

    public static class GivenOrdered_MultiplePerTimestamp_Results
    {
        private static TQuery WithIntervalsCut<TQuery>(
            TQuery query, DateTimeOffset on, OrderDirection queryOrder)
            where TQuery : IQueryWith.Intervals
        {
            Interval? CutLeft(Interval interval)
            {
                if (interval.To <= on)
                    return null;
                return interval.From <= on && interval.To > on ?
                    interval with { From = on } : interval;
            }

            Interval? CutRight(Interval interval)
            {
                if (interval.From >= on)
                    return null;
                return interval.To > on ?
                    interval with { To = on } : interval;
            }

            var newIntervals = query
                .Intervals
                .Select(interval => queryOrder is OrderDirection.Ascending ? CutLeft(interval) : CutRight(interval))
                .Where(inteval => inteval is not null)
                .ToArray();
            var result = query.Copy().Intervals(newIntervals!);
            return result;
        }

        internal sealed class LatestReturned<TEquatableResultPart>
        {
            public DateTimeOffset? Timestamp;
            public Queue<TEquatableResultPart> ResultEquatableParts = new();
        }

        public interface IGetters<TResult, TEquatableResultPart>
        {
            TEquatableResultPart GetEquatablePart(TResult result);
            DateTimeOffset GetTimestamp(TResult result);
            OrderDirection Order { get; }
        }

        internal abstract class ReturnCountBasedAdditionalHandlingState<TQuery>
        {
            public int ReturnCount { get; private set; }
            public void OnReturn() => ReturnCount++;
            public abstract void OnTruncatedResults(TQuery query);
        }

        internal static async IAsyncEnumerable<TResult> Handle<TResult, TEquatableResultPart, TQuery>(
            IAsyncEnumerable<TResult> currentQueryResults,
            TruncatedResultHandlingContext<TSource> context,
            IGetters<TResult, TEquatableResultPart> getters,
            TQuery query,
            ReturnCountBasedAdditionalHandlingState<TQuery>? more,
            [EnumeratorCancellation] CancellationToken token)
            where TQuery : IQueryWith.Intervals, IQueryWith.Source<TSource>
        {
            var latestReturned = context.State.GetOrAdd<LatestReturned<TEquatableResultPart>>();
            var truncated = false;
            var results = OnTruncatedResultsInvoke(currentQueryResults, () => truncated = true, token);
            var timestampChangedAtLeastOnce = false;
            await foreach (var result in results)
            {
                var resultTimestamp = getters.GetTimestamp(result);
                var timestampChanged = latestReturned.Timestamp != resultTimestamp;
                timestampChangedAtLeastOnce = timestampChangedAtLeastOnce || timestampChanged;
                if (timestampChanged)
                {
                    latestReturned.Timestamp = resultTimestamp;
                    latestReturned.ResultEquatableParts.Clear();
                }

                var resultComparablePart = getters.GetEquatablePart(result);
                if (timestampChangedAtLeastOnce || !latestReturned.ResultEquatableParts.Contains(resultComparablePart))
                {
                    latestReturned.ResultEquatableParts.Enqueue(resultComparablePart);
                    more?.OnReturn();
                    yield return result;
                }
            }
            if (!truncated)
                yield break;
            if (latestReturned.Timestamp is not DateTimeOffset existing)
            {
                context.NextQuerySetter = query;
                yield break;
            }

            var copy = WithIntervalsCut(query, existing, getters.Order);
            more?.OnTruncatedResults(copy);
            context.NextQuerySetter = copy;
        }
    }

    public interface TimeSeries<TResult> :
        IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>,
        IQueryWith.Intervals,
        IQueryWith.DescendingFlag,
        IQueryWith.Limit,
        GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, None>
    {
        None GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, None>.GetEquatablePart(WithTimestamp<TResult> result)
            => None.Singleton;
        DateTimeOffset GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, None>.GetTimestamp(WithTimestamp<TResult> result)
            => result.Timestamp;
        OrderDirection GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, None>.Order
            => Descending ? OrderDirection.Descending : OrderDirection.Ascending;

        IAsyncEnumerable<WithTimestamp<TResult>> IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> currentQueryResults, TruncatedResultHandlingContext<TSource> context, CancellationToken token)
            => GivenOrdered_MultiplePerTimestamp_Results.Handle(currentQueryResults, context, this, this, context.State.GetOrAdd<LimitingState>(), token);

        internal sealed class LimitingState : GivenOrdered_MultiplePerTimestamp_Results.ReturnCountBasedAdditionalHandlingState<TimeSeries<TResult>>
        {
            public override void OnTruncatedResults(TimeSeries<TResult> query)
            {
                if (query.Limit is int existing)
                    query.Limit(existing - ReturnCount);
            }
        }
    }

    public interface TopN<TResult, TDimension> :
        IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>,
        IQueryWith.Intervals,
        IDimensionsProvider<TResult, TDimension>,
        GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, TDimension>
        where TDimension : IEquatable<TDimension>
    {
        TDimension GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, TDimension>.GetEquatablePart(WithTimestamp<TResult> result)
            => GetDimensions(result.Value);
        DateTimeOffset GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, TDimension>.GetTimestamp(WithTimestamp<TResult> result)
            => result.Timestamp;
        OrderDirection GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, TDimension>.Order
            => OrderDirection.Ascending;

        IAsyncEnumerable<WithTimestamp<TResult>> IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> currentQueryResults, TruncatedResultHandlingContext<TSource> context, CancellationToken token)
            => GivenOrdered_MultiplePerTimestamp_Results.Handle(currentQueryResults, context, this, this, null, token);
    }

    public interface GroupBy<TResult, TDimensions> :
        IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>,
        IQueryWith.Intervals,
        IQueryWith.LimitSpec,
        IDimensionsProvider<TResult, TDimensions>,
        GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, TDimensions>
        where TDimensions : IEquatable<TDimensions>
    {
        TDimensions GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, TDimensions>.GetEquatablePart(WithTimestamp<TResult> result)
            => GetDimensions(result.Value);
        DateTimeOffset GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, TDimensions>.GetTimestamp(WithTimestamp<TResult> result)
            => result.Timestamp;
        OrderDirection GivenOrdered_MultiplePerTimestamp_Results.IGetters<WithTimestamp<TResult>, TDimensions>.Order
            => OrderDirection.Ascending;

        IAsyncEnumerable<WithTimestamp<TResult>> IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> currentQueryResults, TruncatedResultHandlingContext<TSource> context, CancellationToken token)
            => GivenOrdered_MultiplePerTimestamp_Results.Handle(currentQueryResults, context, this, this, context.State.GetOrAdd<LimitingState>(), token);

        private sealed class LimitingState : GivenOrdered_MultiplePerTimestamp_Results.ReturnCountBasedAdditionalHandlingState<GroupBy<TResult, TDimensions>>
        {
            public override void OnTruncatedResults(GroupBy<TResult, TDimensions> query)
            {
                if (query.Limit is int existing)
                    query.Set(limit: existing - ReturnCount, offset: 0);
            }
        }
    }

    public interface Scan<TResult> :
        IQueryWith.SourceAndResult<TSource, ScanResult<TResult>>,
        IQueryWith.Intervals,
        IQueryWith.OffsetAndLimit,
        IQueryWith.Order
    {
        private sealed class GivenOrderedResults :
            GivenOrdered_MultiplePerTimestamp_Results.ReturnCountBasedAdditionalHandlingState<Scan<TResult>>,
            GivenOrdered_MultiplePerTimestamp_Results.IGetters<ScanResult<TResult>, TResult>
        {
            public override void OnTruncatedResults(Scan<TResult> query)
            {
                if (query.Limit is int existing)
                    query.Limit(existing - ReturnCount).Offset(0);
            }

            public OrderDirection Order { get; }
            private readonly Func<ScanResult<TResult>, DateTimeOffset> getTimestamp;

            private GivenOrderedResults(Func<ScanResult<TResult>, DateTimeOffset> getTimestamp, OrderDirection order)
            {
                this.getTimestamp = getTimestamp;
                Order = order;
            }

            DateTimeOffset GivenOrdered_MultiplePerTimestamp_Results.IGetters<ScanResult<TResult>, TResult>.GetTimestamp(
                ScanResult<TResult> result) => getTimestamp(result);
            public TResult GetEquatablePart(ScanResult<TResult> result) => result.Value;

            private static readonly bool resultImplementsIEquatable
                = typeof(IEquatable<TResult>).IsAssignableFrom(typeof(TResult));

            private static Func<ScanResult<TResult>, DateTimeOffset> GetTimestampGetter(string propertyName)
            {
                var getMethod = typeof(TResult)
                    .GetProperty(propertyName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                    ?.GetMethod
                    ?? throw new InvalidOperationException($"No getter {propertyName} in type {typeof(TResult)}.");
                if (getMethod.ReturnType == typeof(DateTimeOffset))
                {
                    var getter = (Func<TResult, DateTimeOffset>)Delegate.CreateDelegate(typeof(Func<TResult, DateTimeOffset>), getMethod);
                    return result => getter(result.Value);
                }
                else if (getMethod.ReturnType == typeof(DateTime))
                {
                    var getter = (Func<TResult, DateTime>)Delegate.CreateDelegate(typeof(Func<TResult, DateTime>), getMethod);
                    return result => getter(result.Value);
                }
                else
                    throw new NotSupportedException($"Unsupported {DataSourceTimeColumnAttribute.Name} column type {getMethod.ReturnType}.");
            }

            public static IAsyncEnumerable<ScanResult<TResult>>? TryHandle(
                IAsyncEnumerable<ScanResult<TResult>> results, Scan<TResult> query, TruncatedResultHandlingContext<TSource> context, CancellationToken token)
            {
                if (!resultImplementsIEquatable || query.Order is null)
                    return null;
                var resultMappings = context.Mappings.Get<TResult>();
                var timestampProperty = resultMappings
                    .FirstOrDefault(mapping => mapping.ColumnName == DataSourceTimeColumnAttribute.Name)
                    ?.Property;
                if (timestampProperty is null)
                    return null;
                var timestampGetter = GetTimestampGetter(timestampProperty);
                var handling = new GivenOrderedResults(timestampGetter, query.Order.Value);
                return GivenOrdered_MultiplePerTimestamp_Results.Handle(results, context, handling, query, handling, token);
            }
        }

        async IAsyncEnumerable<ScanResult<TResult>> IQueryWith.SourceAndResult<TSource, ScanResult<TResult>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<ScanResult<TResult>> currentQueryResults,
            TruncatedResultHandlingContext<TSource> context,
            [EnumeratorCancellation] CancellationToken token)
        {
            var results = GivenOrderedResults.TryHandle(currentQueryResults, this, context, token);
            if (results is not null)
            {
                await foreach (var result in results)
                    yield return result;
                yield break;
            }

            var state = context.State.GetOrAdd<UnorderedQueryHandlingState>();
            var truncated = false;
            results = OnTruncatedResultsInvoke(currentQueryResults, () => truncated = true, token);
            await foreach (var result in results)
            {
                state.ReturnCount++;
                yield return result;
            }

            if (!truncated || (Limit is not null && state.ReturnCount >= Limit))
                yield break;
            var newQuery = this.Copy().Offset(state.ReturnCount);
            if (Limit is not null)
                newQuery = newQuery.Limit(Limit.Value - state.ReturnCount);
            context.NextQuerySetter = newQuery;
        }

        internal sealed class UnorderedQueryHandlingState
        {
            public int ReturnCount;
        }
    }

    public interface Metadata<TMetadata, TEquatablePart> : 
        IQueryWith.SourceAndResult<TSource, TMetadata>
        where TEquatablePart : IEquatable<TEquatablePart>
    {
        private protected abstract TEquatablePart GetEquatablePart(TMetadata metadata);

        async IAsyncEnumerable<TMetadata> IQueryWith.SourceAndResult<TSource, TMetadata>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TMetadata> currentQueryResults,
            TruncatedResultHandlingContext<TSource> context,
            [EnumeratorCancellation] CancellationToken token)
        {
            var returnedSegmentIds = context.State.GetOrAdd<HashSet<TEquatablePart>>();
            var truncated = false;
            var results = OnTruncatedResultsInvoke(currentQueryResults, () => truncated = true, token);
            await foreach (var result in results)
                if (returnedSegmentIds.Add(GetEquatablePart(result)))
                    yield return result;

            if (truncated)
                context.NextQuerySetter = this;
        }
    }

    public interface SegmentMetadata : Metadata<Querying.SegmentMetadata, string>
    {
        string Metadata<Querying.SegmentMetadata, string>.GetEquatablePart(Querying.SegmentMetadata metadata)
            => metadata.Id;
    }

    public interface DataSourceMetadata : Metadata<WithTimestamp<Querying.DataSourceMetadata>, DateTimeOffset>
    {
        DateTimeOffset Metadata<WithTimestamp<Querying.DataSourceMetadata>, DateTimeOffset>.GetEquatablePart(
            WithTimestamp<Querying.DataSourceMetadata> metadata)
            => metadata.Timestamp;
    }
}
