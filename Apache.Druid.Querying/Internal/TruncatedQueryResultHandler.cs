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
                    interval with { To = on.AddMilliseconds(1) } : // on.AddMilliseconds(1) makes interval right-inclusive
                    interval;
            }

            var newIntervals = query
                .Intervals
                .Select(interval => queryOrder is OrderDirection.Ascending ? CutLeft(interval) : CutRight(interval))
                .Where(inteval => inteval is not null)
                .ToArray();
            var result = query.Copy().Intervals(newIntervals!);
            return result;
        }

        private sealed class LatestReturned<TEquatableResultPart>
        {
            public DateTimeOffset? Timestamp;
            public Queue<TEquatableResultPart> ResultEquatableParts = new();
            public int ResultCount;
        }

        public interface IOrderAccesssor
        {
            OrderDirection Order { get; }
        }

        public interface IQueryAccessors<TQuery>
        {
            int? GetLimit(TQuery query);
            void SetLimit(TQuery query, int? limit);
            int GetOffset(TQuery query);
            void SetOffset(TQuery query, int offset);
        }

        public interface IResultAccessors<TResult, TEquatableResultPart>
        {
            TEquatableResultPart GetEquatablePart(TResult result);
            DateTimeOffset GetTimestamp(TResult result);
        }

        public interface IAccessors<TResult, TEquatableResultPart, TQuery> :
            IOrderAccesssor,
            IQueryAccessors<TQuery>,
            IResultAccessors<TResult, TEquatableResultPart>
        {
        }

        internal static async IAsyncEnumerable<TResult> Handle<TResult, TEquatableResultPart, TQuery>(
            IAsyncEnumerable<TResult> currentQueryResults,
            TruncatedResultHandlingContext<TSource> context,
            IAccessors<TResult, TEquatableResultPart, TQuery> accessors,
            TQuery query,
            [EnumeratorCancellation] CancellationToken token)
            where TQuery : IQueryWith.Intervals, IQueryWith.Source<TSource>
        {
            var latestReturned = context.State.GetOrAdd<LatestReturned<TEquatableResultPart>>();
            var truncated = false;
            var results = OnTruncatedResultsInvoke(currentQueryResults, () => truncated = true, token);
            var timestampChangedAtLeastOnce = false;
            await foreach (var result in results)
            {
                var resultTimestamp = accessors.GetTimestamp(result);
                var timestampChanged = latestReturned.Timestamp != resultTimestamp;
                timestampChangedAtLeastOnce = timestampChangedAtLeastOnce || timestampChanged;
                if (timestampChanged)
                {
                    latestReturned.Timestamp = resultTimestamp;
                    latestReturned.ResultEquatableParts.Clear();
                }

                var resultComparablePart = accessors.GetEquatablePart(result);
                if (timestampChangedAtLeastOnce || !latestReturned.ResultEquatableParts.Contains(resultComparablePart))
                {
                    latestReturned.ResultEquatableParts.Enqueue(resultComparablePart);
                    latestReturned.ResultCount++;
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

            var copy = WithIntervalsCut(query, existing, accessors.Order);
            var limit = accessors.GetLimit(copy);
            var offset = accessors.GetOffset(copy);
            if (limit is not null || offset is not 0)
            {
                accessors.SetLimit(copy, limit is not null ? limit.Value - latestReturned.ResultCount : null);
                accessors.SetOffset(copy, 0);
            }
            context.NextQuerySetter = copy;
        }

        public static class Accessors
        {
            public interface Limit<TQuery> : IQueryAccessors<TQuery> where TQuery : IQueryWith.Limit
            {
                int? IQueryAccessors<TQuery>.GetLimit(TQuery query) => query.Limit;
                void IQueryAccessors<TQuery>.SetLimit(TQuery query, int? limit) => query.Limit = limit;
            }

            public interface NoLimit<TQuery> : IQueryAccessors<TQuery>
            {
                int? IQueryAccessors<TQuery>.GetLimit(TQuery query) => null;
                void IQueryAccessors<TQuery>.SetLimit(TQuery query, int? limit)
                    => _ = limit;
            }

            public interface Offset<TQuery> : IQueryAccessors<TQuery> where TQuery : IQueryWith.Offset
            {
                int IQueryAccessors<TQuery>.GetOffset(TQuery query) => query.Offset;
                void IQueryAccessors<TQuery>.SetOffset(TQuery query, int offset) => query.Offset = offset;
            }

            public interface NoOffset<TQuery> : IQueryAccessors<TQuery>
            {
                int IQueryAccessors<TQuery>.GetOffset(TQuery query) => 0;
                void IQueryAccessors<TQuery>.SetOffset(TQuery query, int offset) => _ = offset;
            }
        }
    }

    public interface TimeSeries<TResult> :
        IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>,
        IQueryWith.Intervals,
        IQueryWith.DescendingFlag,
        IQueryWith.Limit,
        GivenOrdered_MultiplePerTimestamp_Results.IAccessors<WithTimestamp<TResult>, None, TimeSeries<TResult>>,
        GivenOrdered_MultiplePerTimestamp_Results.Accessors.NoOffset<TimeSeries<TResult>>,
        GivenOrdered_MultiplePerTimestamp_Results.Accessors.Limit<TimeSeries<TResult>>
    {
        None GivenOrdered_MultiplePerTimestamp_Results.IResultAccessors<WithTimestamp<TResult>, None>.GetEquatablePart(WithTimestamp<TResult> result)
            => None.Singleton;
        DateTimeOffset GivenOrdered_MultiplePerTimestamp_Results.IResultAccessors<WithTimestamp<TResult>, None>.GetTimestamp(WithTimestamp<TResult> result)
            => result.Timestamp;
        OrderDirection GivenOrdered_MultiplePerTimestamp_Results.IOrderAccesssor.Order
            => Descending ? OrderDirection.Descending : OrderDirection.Ascending;

        IAsyncEnumerable<WithTimestamp<TResult>> IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> currentQueryResults, TruncatedResultHandlingContext<TSource> context, CancellationToken token)
            => GivenOrdered_MultiplePerTimestamp_Results.Handle(currentQueryResults, context, this, this, token);
    }

    public interface TopN<TResult, TDimension> :
        IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>,
        IQueryWith.Intervals,
        IDimensionsProvider<TResult, TDimension>,
        GivenOrdered_MultiplePerTimestamp_Results.IAccessors<WithTimestamp<TResult>, TDimension, TopN<TResult, TDimension>>,
        GivenOrdered_MultiplePerTimestamp_Results.Accessors.NoOffset<TopN<TResult, TDimension>>,
        GivenOrdered_MultiplePerTimestamp_Results.Accessors.NoLimit<TopN<TResult, TDimension>>
        where TDimension : IEquatable<TDimension>
    {
        TDimension GivenOrdered_MultiplePerTimestamp_Results.IResultAccessors<WithTimestamp<TResult>, TDimension>.GetEquatablePart(WithTimestamp<TResult> result)
            => GetDimensions(result.Value);
        DateTimeOffset GivenOrdered_MultiplePerTimestamp_Results.IResultAccessors<WithTimestamp<TResult>, TDimension>.GetTimestamp(WithTimestamp<TResult> result)
            => result.Timestamp;
        OrderDirection GivenOrdered_MultiplePerTimestamp_Results.IOrderAccesssor.Order
            => OrderDirection.Ascending;

        IAsyncEnumerable<WithTimestamp<TResult>> IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> currentQueryResults, TruncatedResultHandlingContext<TSource> context, CancellationToken token)
            => GivenOrdered_MultiplePerTimestamp_Results.Handle(currentQueryResults, context, this, this, token);
    }

    public interface GroupBy<TResult, TDimensions> :
        IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>,
        IQueryWith.Intervals,
        IQueryWith.Limit,
        IQueryWith.Offset,
        IDimensionsProvider<TResult, TDimensions>,
        GivenOrdered_MultiplePerTimestamp_Results.IAccessors<WithTimestamp<TResult>, TDimensions, GroupBy<TResult, TDimensions>>,
        GivenOrdered_MultiplePerTimestamp_Results.Accessors.Offset<GroupBy<TResult, TDimensions>>,
        GivenOrdered_MultiplePerTimestamp_Results.Accessors.Limit<GroupBy<TResult, TDimensions>>
        where TDimensions : IEquatable<TDimensions>
    {
        TDimensions GivenOrdered_MultiplePerTimestamp_Results.IResultAccessors<WithTimestamp<TResult>, TDimensions>.GetEquatablePart(WithTimestamp<TResult> result)
            => GetDimensions(result.Value);
        DateTimeOffset GivenOrdered_MultiplePerTimestamp_Results.IResultAccessors<WithTimestamp<TResult>, TDimensions>.GetTimestamp(WithTimestamp<TResult> result)
            => result.Timestamp;
        OrderDirection GivenOrdered_MultiplePerTimestamp_Results.IOrderAccesssor.Order
            => OrderDirection.Ascending;

        IAsyncEnumerable<WithTimestamp<TResult>> IQueryWith.SourceAndResult<TSource, WithTimestamp<TResult>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> currentQueryResults, TruncatedResultHandlingContext<TSource> context, CancellationToken token)
            => GivenOrdered_MultiplePerTimestamp_Results.Handle(currentQueryResults, context, this, this, token);
    }

    public interface Scan<TResult> :
        IQueryWith.SourceAndResult<TSource, ScanResult<TResult>>,
        IQueryWith.Intervals,
        IQueryWith.Offset,
        IQueryWith.Limit,
        IQueryWith.Order
    {
        private sealed class GivenOrderedResults :
            GivenOrdered_MultiplePerTimestamp_Results.IAccessors<ScanResult<TResult>, TResult, Scan<TResult>>
        {
            public OrderDirection Order { get; }
            private readonly Func<ScanResult<TResult>, DateTimeOffset> getTimestamp;

            private GivenOrderedResults(Func<ScanResult<TResult>, DateTimeOffset> getTimestamp,  OrderDirection order)
            {
                this.getTimestamp = getTimestamp;
                Order = order;
            }

            public int? GetLimit(Scan<TResult> query) => query.Limit;
            public void SetLimit(Scan<TResult> query, int? limit) => query.Limit = limit;
            public int GetOffset(Scan<TResult> query) => query.Offset;
            public void SetOffset(Scan<TResult> query, int offset) => query.Offset = offset;    

            DateTimeOffset GivenOrdered_MultiplePerTimestamp_Results.IResultAccessors<ScanResult<TResult>, TResult>.GetTimestamp(
                ScanResult<TResult> result) => getTimestamp(result);
            TResult GivenOrdered_MultiplePerTimestamp_Results.IResultAccessors<ScanResult<TResult>, TResult>.GetEquatablePart(
                ScanResult<TResult> result) => result.Value;

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

            private static readonly GivenOrderedResults notApplicableSentinel = new(static _ => throw new NotSupportedException(), default);
            public static IAsyncEnumerable<ScanResult<TResult>>? TryHandle(
                IAsyncEnumerable<ScanResult<TResult>> results, Scan<TResult> query, TruncatedResultHandlingContext<TSource> context, CancellationToken token)
            {
                GivenOrderedResults Create()
                {
                    if (!resultImplementsIEquatable || query.Order is null)
                        return notApplicableSentinel;
                    var resultMappings = context.Mappings.Get<TResult>();
                    var timestampProperty = resultMappings
                        .FirstOrDefault(mapping => mapping.ColumnName == DataSourceTimeColumnAttribute.Name)
                        ?.Property;
                    if (timestampProperty is null)
                        return notApplicableSentinel;
                    var timestampGetter = GetTimestampGetter(timestampProperty);
                    return new GivenOrderedResults(timestampGetter, query.Order.Value);
                }

                if (!context.State.TryGet<GivenOrderedResults>(out var handling))
                {
                    handling = Create();
                    context.State.Add(handling);
                }

                return handling == notApplicableSentinel ? 
                    null :
                    GivenOrdered_MultiplePerTimestamp_Results.Handle(results, context, handling, query, token);
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
            var newQuery = this.Copy().Offset(Offset + state.ReturnCount);
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
