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

    public class Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp_Context<TEquatableResultPart>
    {
        public DateTimeOffset? Timestamp;
        public Queue<TEquatableResultPart> ResultEquatableParts = new();
    }

    private static async IAsyncEnumerable<TResult> Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp<
        TResult, TEquatableResultPart, TContext, TQuery>(
        IAsyncEnumerable<TResult> results,
        TContext latestReturned,
        Mutable<IQueryWith.Source<TSource>> setter,
        Func<TResult, TEquatableResultPart> getEquatablePart,
        Func<TResult, DateTimeOffset> getTimestamp,
        TQuery query,
        [EnumeratorCancellation] CancellationToken token)
        where TContext : Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp_Context<TEquatableResultPart>
        where TQuery : IQueryWith.Intervals, IQueryWith.Source<TSource>
    {
        var truncated = false;
        results = OnTruncatedResultsInvoke(results, () => truncated = true, token);
        var timestampChangedAtLeastOnce = false;
        await foreach (var result in results)
        {
            var resultTimestamp = getTimestamp(result);
            var timestampChanged = latestReturned.Timestamp != resultTimestamp;
            timestampChangedAtLeastOnce = timestampChangedAtLeastOnce || timestampChanged;
            if (timestampChanged)
            {
                latestReturned.Timestamp = resultTimestamp;
                latestReturned.ResultEquatableParts.Clear();
            }

            var resultComparablePart = getEquatablePart(result);
            if (timestampChangedAtLeastOnce || !latestReturned.ResultEquatableParts.Contains(resultComparablePart))
            {
                latestReturned.ResultEquatableParts.Enqueue(resultComparablePart);
                yield return result;
            }
        }

        if (!truncated)
            yield break;
        setter.Value = latestReturned.Timestamp is DateTimeOffset existing ? WithIntervalsStartingFrom(query, existing) : query;
    }

    public interface TimeSeries<TResult> :
        Base<WithTimestamp<TResult>, TimeSeries<TResult>.LatestReturned>
    {
        public sealed class LatestReturned
        {
            public DateTimeOffset? Timestamp;
        }

        async IAsyncEnumerable<WithTimestamp<TResult>> Base<WithTimestamp<TResult>, LatestReturned>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> results,
            LatestReturned latestReturned,
            Mutable<IQueryWith.Source<TSource>> setter,
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

    public interface TopN_GroupBy<TResult, TDimensions> :
        Base<WithTimestamp<TResult>, Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp_Context<TDimensions>>,
        IDimensionsProvider<TResult, TDimensions>
        where TDimensions : IEquatable<TDimensions>
    {
        IAsyncEnumerable<WithTimestamp<TResult>> Base<WithTimestamp<TResult>, Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp_Context<TDimensions>>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<WithTimestamp<TResult>> results,
            Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp_Context<TDimensions> context,
            Mutable<IQueryWith.Source<TSource>> setter,
            CancellationToken token)
            => Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp(
                results,
                context,
                setter,
                result => GetDimensions(result.Value),
                static result => result.Timestamp,
                this,
                token);
    }

    public interface Scan<TResult> :
        Base<ScanResult<TResult>, Scan<TResult>.LatestResult>,
        IQueryWith.OffsetAndLimit,
        IQueryWith.Order
    {
        public sealed class LatestResult : Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp_Context<TResult>
        {
            public int Count;
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

        async IAsyncEnumerable<ScanResult<TResult>> Base<ScanResult<TResult>, LatestResult>.OnTruncatedResultsSetQueryForRemaining(
            IAsyncEnumerable<ScanResult<TResult>> results,
            LatestResult latestResult,
            Mutable<IQueryWith.Source<TSource>> setter,
            PropertyColumnNameMapping.IProvider mappings,
            [EnumeratorCancellation] CancellationToken token)
        {
            string? TryGetTimestampProperty() 
            {
                if (!resultImplementsIEquatable || Order is null)
                    return null;
                var resultMappings = mappings.Get<TResult>();
                return resultMappings
                    .FirstOrDefault(mapping => mapping.ColumnName == DataSourceTimeColumnAttribute.Name)
                    ?.Property;
            }

            latestResult.GetTimestamp ??= TryGetTimestampProperty() is string property ?  GetTimestampGetter(property) : null;
            if (latestResult.GetTimestamp is not null)
            {
                results = Implementation_WithTimeOrderedResults_AndMultipleResultsPerTimestamp(
                    results,
                    latestResult,
                    setter,
                    static result => result.Value,
                    latestResult.GetTimestamp,
                    this,
                    token);
                await foreach (var result in results)
                    yield return result;
                yield break;
            }

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
