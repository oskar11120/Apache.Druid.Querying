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

    public interface Base<TResult, TContext> :
        IQueryWith.SourceAndResult<TSource, TResult>,
        IQueryWith.Intervals
        where TContext : new()
    {
        IAsyncEnumerable<TResult> IQueryWith.SourceAndResult<TSource, TResult>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TResult> results,
            TruncatedQueryResultHandlingContext context,
            Mutable<IQueryWith.Source<TSource>> setter,
            CancellationToken token)
        {
            context.State ??= new TContext();
            return OnTruncatedResultsSetQueryForRemaining(results, (TContext)context.State, setter, context.ColumnNameMappings, token);
        }

        internal IAsyncEnumerable<TResult> OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TResult> results,
            TContext context,
            Mutable<IQueryWith.Source<TSource>> setter,
            PropertyColumnNameMapping.IProvider mappings,
            CancellationToken token)
            => OnTruncatedResultsSetQueryForRemaining(results, context, setter, token);

        internal IAsyncEnumerable<TResult> OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TResult> results, TContext context, Mutable<IQueryWith.Source<TSource>> setter, CancellationToken token)
            => throw new NotSupportedException();
    }

    public interface WithOrdered_MultiplePerTimestamp_Results<TResult, TEquatableResultPart, TContext>
        : Base<TResult, TContext>
        where TContext : WithOrdered_MultiplePerTimestamp_Results<TResult, TEquatableResultPart, TContext>.Context, new()
    {
        public class Context
        {
            internal DateTimeOffset? Timestamp;
            internal Queue<TEquatableResultPart> ResultEquatableParts = new();
            internal virtual void OnReturn(TResult result)
            {
            }
        }

        private protected abstract TEquatableResultPart GetEquatablePart(TResult result);
        private protected abstract DateTimeOffset GetTimestamp(TResult result);
        private protected abstract OrderDirection Order { get; }
        private protected virtual void OnTruncatedResults(TContext context)
        {
        }

        async IAsyncEnumerable<TResult> Base<TResult, TContext>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<TResult> results,
            TContext latestReturned,
            Mutable<IQueryWith.Source<TSource>> setter,
            [EnumeratorCancellation] CancellationToken token)
        {
            var truncated = false;
            results = OnTruncatedResultsInvoke(results, () => truncated = true, token);
            var timestampChangedAtLeastOnce = false;
            await foreach (var result in results)
            {
                var resultTimestamp = GetTimestamp(result);
                var timestampChanged = latestReturned.Timestamp != resultTimestamp;
                timestampChangedAtLeastOnce = timestampChangedAtLeastOnce || timestampChanged;
                if (timestampChanged)
                {
                    latestReturned.Timestamp = resultTimestamp;
                    latestReturned.ResultEquatableParts.Clear();
                }

                var resultComparablePart = GetEquatablePart(result);
                if (timestampChangedAtLeastOnce || !latestReturned.ResultEquatableParts.Contains(resultComparablePart))
                {
                    latestReturned.ResultEquatableParts.Enqueue(resultComparablePart);
                    latestReturned.OnReturn(result);
                    yield return result;
                }
            }
            if (!truncated)
                yield break;
            if (latestReturned.Timestamp is not DateTimeOffset existing)
            {
                setter.Value = this;
                yield break;
            }

            var copy = WithIntervalsCut(this, existing, Order);
            copy.OnTruncatedResults(latestReturned);
            setter.Value = copy;
        }

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
    }

    public interface WithOrdered_MultiplePerTimestamp_Results<TResult, TEquatableResultPart>
        : WithOrdered_MultiplePerTimestamp_Results<
            TResult,
            TEquatableResultPart,
            WithOrdered_MultiplePerTimestamp_Results<TResult, TEquatableResultPart>.Context_>
    {
        public sealed class Context_ : Context
        {
        }
    }

    public interface WithOrdered_MultiplePerTimestamp_Results_AndLimit<TResult, TEquatableResultPart, TContext>
        : WithOrdered_MultiplePerTimestamp_Results<TResult, TEquatableResultPart, TContext>
        where TContext : WithOrdered_MultiplePerTimestamp_Results_AndLimit<TResult, TEquatableResultPart, TContext>.Context_, new()
    {
        public class Context_ : Context
        {
            internal int ReturnCount;
            internal override void OnReturn(TResult result) => ReturnCount++;
        }

        private protected abstract void SetLimit(int returnCount);

        void WithOrdered_MultiplePerTimestamp_Results<TResult, TEquatableResultPart, TContext>.OnTruncatedResults(TContext context)
            => SetLimit(context.ReturnCount);
    }

    public interface WithOrdered_MultiplePerTimestamp_Results_AndLimit<TResult, TEquatableResultPart>
        : WithOrdered_MultiplePerTimestamp_Results_AndLimit<
            TResult,
            TEquatableResultPart,
            WithOrdered_MultiplePerTimestamp_Results_AndLimit<TResult, TEquatableResultPart>.Context__>
    {
        public sealed class Context__ : Context_
        {
        }
    }

    public interface TimeSeries<TResult> :
        WithOrdered_MultiplePerTimestamp_Results_AndLimit<WithTimestamp<TResult>, TimeSeries<TResult>.None>,
        IQueryWith.DescendingFlag,
        IQueryWith.Limit
    {
        public readonly struct None
        {
        }

        None WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, None, Context__>.GetEquatablePart(
            WithTimestamp<TResult> result) => default;
        DateTimeOffset WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, None, Context__>.GetTimestamp(
            WithTimestamp<TResult> result) => result.Timestamp;
        OrderDirection WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, None, Context__>.Order
            => Descending ? OrderDirection.Descending : OrderDirection.Ascending;

        void WithOrdered_MultiplePerTimestamp_Results_AndLimit<WithTimestamp<TResult>, None, Context__>.SetLimit(int returnCount)
        {
            if (Limit is int existing)
                this.Limit(existing - returnCount);
        }
    }

    public interface TopN<TResult, TDimension> :
        WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, TDimension>,
        IDimensionsProvider<TResult, TDimension>
        where TDimension : IEquatable<TDimension>
    {
        TDimension WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, TDimension, Context_>.GetEquatablePart(
            WithTimestamp<TResult> result) => GetDimensions(result.Value);
        DateTimeOffset WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, TDimension, Context_>.GetTimestamp(
            WithTimestamp<TResult> result) => result.Timestamp;
        OrderDirection WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, TDimension, Context_>.Order
            => OrderDirection.Ascending;
    }

    public interface GroupBy<TResult, TDimensions> :
        IQueryWith.LimitSpec,
        WithOrdered_MultiplePerTimestamp_Results_AndLimit<WithTimestamp<TResult>, TDimensions>,
        IDimensionsProvider<TResult, TDimensions>
        where TDimensions : IEquatable<TDimensions>
    {
        TDimensions WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, TDimensions, Context__>.GetEquatablePart(
            WithTimestamp<TResult> result) => GetDimensions(result.Value);
        DateTimeOffset WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, TDimensions, Context__>.GetTimestamp(
            WithTimestamp<TResult> result) => result.Timestamp;
        OrderDirection WithOrdered_MultiplePerTimestamp_Results<WithTimestamp<TResult>, TDimensions, Context__>.Order
            => OrderDirection.Ascending;

        void WithOrdered_MultiplePerTimestamp_Results_AndLimit<WithTimestamp<TResult>, TDimensions, Context__>.SetLimit(int returnCount)
        {
            if (Limit is int existing)
                Set(limit: existing - returnCount, offset: 0);
        }
    }

    public interface Scan<TResult> :
        WithOrdered_MultiplePerTimestamp_Results_AndLimit<ScanResult<TResult>, TResult, Scan<TResult>.Context>,
        IQueryWith.OffsetAndLimit,
        IQueryWith.Order
    {
        public new sealed class Context : Context_
        {
            public Func<ScanResult<TResult>, DateTimeOffset>? GetTimestamp;
        }

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

        async IAsyncEnumerable<ScanResult<TResult>> Base<ScanResult<TResult>, Context>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<ScanResult<TResult>> results,
            Context latestResult,
            Mutable<IQueryWith.Source<TSource>> setter,
            PropertyColumnNameMapping.IProvider mappings,
            [EnumeratorCancellation] CancellationToken token)
        {
            string? TryGetTimestampProperty()
            {
                if (!resultImplementsIEquatable || (this as IQueryWith.Order).Order is null)
                    return null;
                var resultMappings = mappings.Get<TResult>();
                return resultMappings
                    .FirstOrDefault(mapping => mapping.ColumnName == DataSourceTimeColumnAttribute.Name)
                    ?.Property;
            }

            latestResult.GetTimestamp ??= TryGetTimestampProperty() is string property ? GetTimestampGetter(property) : null;
            if (latestResult.GetTimestamp is not null)
            {
                results = OnTruncatedResultsSetQueryForRemaining(results, latestResult, setter, token);
                await foreach (var result in results)
                    yield return result;
                yield break;
            }

            var truncated = false;
            results = OnTruncatedResultsInvoke(results, () => truncated = true, token);
            await foreach (var result in results)
            {
                latestResult.ReturnCount++;
                yield return result;
            }

            if (!truncated || (Limit is not null && latestResult.ReturnCount >= Limit))
                yield break;
            var newQuery = this.Copy().Offset(latestResult.ReturnCount);
            if (Limit is not null)
                newQuery = newQuery.Limit(Limit.Value - latestResult.ReturnCount);
            setter.Value = newQuery;
        }
    }

    public interface SegmentMetadata : Base<Querying.SegmentMetadata, HashSet<string>>
    {
        async IAsyncEnumerable<Querying.SegmentMetadata> Base<Querying.SegmentMetadata, HashSet<string>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<Querying.SegmentMetadata> results,
            HashSet<string> returnedSegmentIds,
            Mutable<IQueryWith.Source<TSource>> setter,
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
