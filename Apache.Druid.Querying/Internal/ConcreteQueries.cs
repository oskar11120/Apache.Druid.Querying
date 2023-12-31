using Apache.Druid.Querying.Internal.QuerySectionFactory;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Druid.Querying.Internal
{
    public static class IQueryWithMappedResult
    {
        public interface WithTimestampArray<TValue, TValueMapper>
            : IQueryWithMappedResult<WithTimestamp<TValue>,
            QueryResultMapper.Array<WithTimestamp<TValue>, QueryResultMapper.WithTimestamp<TValue, TValueMapper>>>
            where TValueMapper : IQueryResultMapper<TValue>, new()
        {
        }

        public interface GroupByResultArray<TValue, TValueMapper>
            : IQueryWithMappedResult<WithTimestamp<TValue>,
            QueryResultMapper.Array<WithTimestamp<TValue>, QueryResultMapper.GroupByResult<TValue, TValueMapper>>>
            where TValueMapper : IQueryResultMapper<TValue>, new()
        {
        }

        public static class WithTimestamp
        {
            public interface Aggregations_PostAggregations_<TAggregations, TPostAggregations> : WithTimestampArray<
                Aggregations_PostAggregations<TAggregations, TPostAggregations>,
                QueryResultMapper.Aggregations_PostAggregations_<TAggregations, TPostAggregations>>
            {
            }

            public interface Dimension_Aggregations_<TDimension, TAggregations> : WithTimestampArray<
                Dimension_Aggregations<TDimension, TAggregations>,
                QueryResultMapper.Array<
                    Dimension_Aggregations<TDimension, TAggregations>,
                    QueryResultMapper.Dimension_Aggregations_<TDimension, TAggregations>>>
            {
            }

            public interface Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations> : WithTimestampArray<
                Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>,
                QueryResultMapper.Array<
                    Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>,
                    QueryResultMapper.Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>>>
            {
            }

            public interface Dimensions_Aggregations_<TDimensions, TAggregations> : GroupByResultArray<
                Dimensions_Aggregations<TDimensions, TAggregations>,
                QueryResultMapper.Array<
                    Dimensions_Aggregations<TDimensions, TAggregations>,
                    QueryResultMapper.Dimensions_Aggregations_<TDimensions, TAggregations>>>
            {
            }

            public interface Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations> : GroupByResultArray<
                Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>,
                QueryResultMapper.Array<
                    Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>,
                    QueryResultMapper.Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>>>
            {
            }
        }
    }

    public static class QueryResultMapper
    {
        public abstract class WithTwoProperties<TFirstNonMappable, TSecondMappable, TScondMapper, TResult> :
            IQueryResultMapper<TResult>
            where TScondMapper : IQueryResultMapper<TSecondMappable>, new()
        {
            private static readonly IQueryResultMapper<TSecondMappable> mapper = new TScondMapper();
            private readonly (byte[] First, byte[] Second) names;
            private readonly Func<TFirstNonMappable, TSecondMappable, TResult> create;

            public WithTwoProperties(string firstName, string secondName, Func<TFirstNonMappable, TSecondMappable, TResult> create)
            {
                names = (ToJson(firstName), ToJson(secondName));
                this.create = create;
            }

            async IAsyncEnumerable<TResult> IQueryResultMapper<TResult>.Map(
                QueryResultMapperContext context, [EnumeratorCancellation] CancellationToken token)
            {
                var (json, _, _) = context;
                TFirstNonMappable first;
                while (!json.ReadToPropertyValue(names.First, out first))
                    await json.AdvanceAsync(token);
                while (!json.ReadToProperty(names.Second))
                    await json.AdvanceAsync(token);

                var results = mapper.Map(context, token);
                await foreach (var result in results)
                    yield return create(first, result);

                while (!json.ReadToTokenType(JsonTokenType.EndObject))
                    await json.AdvanceAsync(token);
            }

            private static byte[] ToJson(string propertyName) =>
                Encoding.UTF8.GetBytes(propertyName.ToCamelCase());
        }

        public class WithTimestamp<TResult, TResultMapper>
            : WithTwoProperties<DateTimeOffset, TResult, TResultMapper, WithTimestamp<TResult>>
            where TResultMapper : IQueryResultMapper<TResult>, new()
        {
            public WithTimestamp() : this("result")
            {            
            }

            public WithTimestamp(string valuePropertyNameBase)
                : base(nameof(WithTimestamp<TResult>.Timestamp), valuePropertyNameBase, static (t, result) => new(t, result))
            {
            }
        }

        public sealed class GroupByResult<TEvent, TEventMapper>
            : WithTimestamp<TEvent, TEventMapper>
            where TEventMapper : IQueryResultMapper<TEvent>, new()
        {
            public GroupByResult() : base("event")
            {
            }
        }

        public sealed class Array<TElement, TElementMapper> :
            IQueryResultMapper<TElement>
            where TElementMapper : IQueryResultMapper<TElement>, new()
        {
            private static readonly IQueryResultMapper<TElement> mapper = new TElementMapper();

            async IAsyncEnumerable<TElement> IQueryResultMapper<TElement>.Map(
                QueryResultMapperContext context, [EnumeratorCancellation] CancellationToken token)
            {
                var (json, _, _) = context;
                bool SkipEndArray(out bool skippedEndArray)
                {
                    var reader = json.GetReader();
                    var read = reader.Read();
                    if (!read)
                    {
                        skippedEndArray = default;
                        return false;
                    }

                    skippedEndArray = reader.TokenType is JsonTokenType.EndArray;
                    if (skippedEndArray)
                        json.UpdateState(reader);
                    return true;
                }

                while (!json.ReadToTokenType(JsonTokenType.StartArray))
                    await json.AdvanceAsync(token);
                while (true)
                {
                    bool skippedEndArray;
                    while (!SkipEndArray(out skippedEndArray))
                        await json.AdvanceAsync(token);
                    if (skippedEndArray)
                        yield break;

                    var results = mapper.Map(context, token);
                    await foreach (var result in results)
                        yield return result;

                    while (!SkipEndArray(out skippedEndArray))
                        await json.AdvanceAsync(token);
                    if (skippedEndArray)
                        yield break;
                }
            }
        }

        // "Atoms" are objects small enough that whole their data can be fit into buffer. 
        public abstract class Atom<TSelf> : IQueryResultMapper<TSelf>
        {
            async IAsyncEnumerable<TSelf> IQueryResultMapper<TSelf>.Map(
                QueryResultMapperContext context, [EnumeratorCancellation] CancellationToken token)
            {
                var (json, _, _) = context;
                var bytes = await EnsureWholeInBufferAndGetSpanningBytesAsync(json, token);
                TSelf Map_()
                {
                    var context_ = new Context(context, bytes);
                    var result = Map(ref context_);
                    context_.UpdateState();
                    return result;
                }

                yield return Map_();
            }

            private protected abstract TSelf Map(ref Context context);

            private static async ValueTask<int> EnsureWholeInBufferAndGetSpanningBytesAsync(JsonStreamReader json, CancellationToken token)
            {
                long bytesConsumed;
                while (!json.ReadToTokenTypeAtNextTokenDepth(JsonTokenType.EndObject, out bytesConsumed, false))
                    await json.AdvanceAsync(token);
                return (int)bytesConsumed;
            }

            protected private ref struct Context
            {
                private static readonly byte[] comaBytes = Encoding.UTF8.GetBytes(",");
                private readonly JsonStreamReader json;
                private readonly JsonSerializerOptions options;
                private readonly SectionAtomicity.IProvider atomicity;
                private readonly int spanningBytes;
                private readonly int trimBytes;
                private long deserializeConsumedBytes = 0;

                public Context(QueryResultMapperContext mapperContext, int spanningBytes)
                {
                    (json, options, atomicity) = mapperContext;
                    this.spanningBytes = spanningBytes;

                    var startWithComa = json.UnreadPartOfBufferStartsWith(comaBytes);
                    trimBytes = startWithComa ? comaBytes.Length : 0;
                }

                public TObject Deserialize<TObject>()
                {
                    var (atomic, _, columnName) = atomicity.Get<TObject>();
                    var reader = json.GetReaderForSlice(spanningBytes, trimBytes);
                    if (atomic)
                    {
                        JsonStreamReader.ReadToProperty(ref reader, columnName);
                        var start = (int)reader.BytesConsumed;
                        var propertyDepth = reader.CurrentDepth;
                        do
                            reader.Read();
                        while (reader.CurrentDepth < propertyDepth);
                        deserializeConsumedBytes = reader.BytesConsumed;
                        reader = json.GetReaderForSlice((int)deserializeConsumedBytes + trimBytes, start + trimBytes);
                        return JsonSerializer.Deserialize<TObject>(ref reader, options)!;
                    }

                    var result = JsonSerializer.Deserialize<TObject>(ref reader, options)!;
                    deserializeConsumedBytes = reader.BytesConsumed;
                    return result;
                }

                public readonly void UpdateState()
                {
                    var reader = json.GetReader();
                    while (reader.BytesConsumed < deserializeConsumedBytes + trimBytes)
                        reader.Read();
                    json.UpdateState(reader);
                }
            }
        }

        public sealed class Aggregations_PostAggregations_<TAggregations, TPostAggregations>
            : Atom<Aggregations_PostAggregations<TAggregations, TPostAggregations>>
        {
            private protected override Aggregations_PostAggregations<TAggregations, TPostAggregations> Map(ref Context context)
                => new(
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        }

        public sealed class Dimension_Aggregations_<TDimension, TAggregations> :
            Atom<Dimension_Aggregations<TDimension, TAggregations>>
        {
            private protected override Dimension_Aggregations<TDimension, TAggregations> Map(ref Context context)
                => new(
                    context.Deserialize<TDimension>(),
                    context.Deserialize<TAggregations>());
        }

        public sealed class Dimension_Aggregations_PostAggregations_<TDimension, TAggregations, TPostAggregations>
             : Atom<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>
        {
            private protected override Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations> Map(ref Context context)
                => new(
                    context.Deserialize<TDimension>(),
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        }

        public sealed class Dimensions_Aggregations_<TDimensions, TAggregations>
            : Atom<Dimensions_Aggregations<TDimensions, TAggregations>>
        {
            private protected override Dimensions_Aggregations<TDimensions, TAggregations> Map(ref Context context)
                => new(
                    context.Deserialize<TDimensions>(),
                    context.Deserialize<TAggregations>());
        }

        public sealed class Dimensions_Aggregations_PostAggregations_<TDimensions, TAggregations, TPostAggregations>
             : Atom<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>
        {
            private protected override Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations> Map(ref Context context)
                => new(
                    context.Deserialize<TDimensions>(),
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        }
    }

    public static class Marker
    {
        public sealed record Dimension;
        public sealed record Dimensions;
    }

    public static class QueryBase<TArguments, TSelf> where TSelf : IQuery<TSelf>
    {
        public abstract class TimeSeries :
            Query,
            IQueryWith.Order,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TimeSeries, TSelf>
        {
            public TimeSeries() : base("timeseries")
            {
            }
        }

        public abstract class TimeSeries<TAggregations> :
            TimeSeries,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class TimeSeries<TAggregations, TPostAggregations> :
            TimeSeries<TAggregations>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }

        private static readonly SectionFactoryJsonMapper.Options dimensionsMapperOptions = new(SectionColumnNameKey: "outputName");
        public abstract class TopN_<TDimension, TMetricArguments> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.TopN, TSelf>,
            IQuery<TArguments, TSelf, Marker.Dimension>
        {
            private static readonly SectionFactoryJsonMapper.Options mapperOptions = dimensionsMapperOptions with { ForceSingle = true };

            public TopN_() : base("topN")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimension(Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimension>> factory)
                => this.AddOrUpdateSectionWithSectionFactory(nameof(Dimension), factory, mapperOptions);

            public TSelf Threshold(int threshold)
                => Self.AddOrUpdateSection(nameof(threshold), threshold);

            public TSelf Metric(Func<QueryElementFactory<TMetricArguments>.MetricSpec, IMetric> factory)
                => Self.AddOrUpdateSection(nameof(Metric), columnNames => factory(new(columnNames)));
        }

        public abstract class TopN<TDimension> : TopN_<TDimension, TDimension>
        {
        }

        public abstract class TopN<TDimension, TAggregations> :
            TopN_<TDimension, Dimension_Aggregations<TDimension, TAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class TopN<TDimension, TAggregations, TPostAggregations> :
            TopN_<TDimension, Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }

        public abstract class GroupBy_<TDimensions, TOrderByAndHavingArguments> :
            Query,
            IQueryWith.Intervals,
            IQueryWith.Granularity,
            IQueryWith.Filter<TArguments, TSelf>,
            IQueryWith.Context<QueryContext.GroupBy, TSelf>,
            IQuery<TArguments, TSelf, Marker.Dimensions>
        {
            public GroupBy_() : base("groupBy")
            {
            }

            private IQuery<TSelf> Self => this;

            public TSelf Dimensions(Expression<QuerySectionFactory<QueryElementFactory<TArguments>.IDimensions, TDimensions>> factory)
                => this.AddOrUpdateSectionWithSectionFactory(nameof(Dimensions), factory, dimensionsMapperOptions);

            public TSelf LimitSpec(
                int? limit = null,
                int? offset = null,
                Func<QueryElementFactory<TOrderByAndHavingArguments>.OrderByColumnSpec, IEnumerable<ILimitSpec.OrderBy>>? columns = null)
                => Self.AddOrUpdateSection(nameof(LimitSpec), columnNames => new LimitSpec(limit, offset, columns?.Invoke(new(columnNames))));

            public TSelf Having(Func<QueryElementFactory<TOrderByAndHavingArguments>.Having, IHaving> factory)
                => Self.AddOrUpdateSection(nameof(Having), columnNames => factory(new(columnNames)));

            public TSelf HavingFilter(Func<QueryElementFactory<TOrderByAndHavingArguments>.Filter, IFilter> factory)
                => Self.AddOrUpdateSection(nameof(Having), columnNames => new QueryElementFactory<TOrderByAndHavingArguments>.Having(columnNames).Filter(factory));
        }

        public abstract class GroupBy<TDimensions> : GroupBy_<TDimensions, TDimensions>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations<TDimensions, TAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>
        {
        }

        public abstract class GroupBy<TDimensions, TAggregations, TPostAggregations> :
            GroupBy_<TDimensions, Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>>,
            IQueryWith.Aggregations<TArguments, TAggregations, TSelf>,
            IQueryWith.PostAggregations<TAggregations, TPostAggregations, TSelf>
        {
        }
    }
}
