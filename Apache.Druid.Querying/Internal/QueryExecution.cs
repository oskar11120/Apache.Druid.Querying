using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Druid.Querying.Internal
{
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
                var json = context.Json;
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

        public sealed class Array<TElement, TElementMapper> :
            IQueryResultMapper<TElement>
            where TElementMapper : IQueryResultMapper<TElement>, new()
        {
            private static readonly IQueryResultMapper<TElement> mapper = new TElementMapper();

            async IAsyncEnumerable<TElement> IQueryResultMapper<TElement>.Map(
                QueryResultMapperContext context, [EnumeratorCancellation] CancellationToken token)
            {
                var json = context.Json;
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
                var json = context.Json;
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
                private readonly int spanningBytes;
                private readonly int trimBytes;
                private long deserializeConsumedBytes = 0;
                public readonly QueryResultMapperContext MapperContext;

                public Context(QueryResultMapperContext mapperContext, int spanningBytes)
                {
                    MapperContext = mapperContext;
                    this.spanningBytes = spanningBytes;

                    var startWithComa = mapperContext.Json.UnreadPartOfBufferStartsWith(comaBytes);
                    trimBytes = startWithComa ? comaBytes.Length : 0;
                }

                private static Utf8JsonReader WrapInReader(ReadOnlySpan<byte> slice)
                    => new(slice, false, default);

                public TObject Deserialize<TObject>(bool checkForAtomicity = true)
                {
                    var (json, options, atomicity, _) = MapperContext;
                    var slice = json.GetSliceOfBuffer(spanningBytes, trimBytes);
                    deserializeConsumedBytes = slice.Length;
                    SectionAtomicity atomicity_;
                    if (checkForAtomicity && (atomicity_ = atomicity.Get<TObject>()).Atomic)
                    {
                        var reader = WrapInReader(slice);
                        JsonStreamReader.ReadToProperty(ref reader, atomicity_.ColumnNameIfAtomicUtf8);
                        var start = (int)reader.BytesConsumed;
                        var propertyDepth = reader.CurrentDepth;
                        do
                            reader.Read();
                        while (reader.CurrentDepth < propertyDepth);
                        slice = slice[start..(int)reader.BytesConsumed];
                    }

                    var result = JsonSerializer.Deserialize<TObject>(slice, options)!;
                    return result;
                }

                public readonly void UpdateState()
                {
                    var json = MapperContext.Json;
                    var reader = json.GetReader();
                    while (reader.BytesConsumed < deserializeConsumedBytes + trimBytes)
                        reader.Read();
                    json.UpdateState(reader);
                }
            }
        }

        public class WithTimestamp<TValue, TValueMapper>
            : WithTwoProperties<DateTimeOffset, TValue, TValueMapper, WithTimestamp<TValue>>
            where TValueMapper : IQueryResultMapper<TValue>, new()
        {
            public WithTimestamp() : this("result")
            {
            }

            public WithTimestamp(string valuePropertyNameBase)
                : base(nameof(WithTimestamp<TValue>.Timestamp), valuePropertyNameBase, static (t, value) => new(t, value))
            {
            }
        }

        public sealed class GroupByResult<TValue, TValueMapper>
            : WithTimestamp<TValue, TValueMapper>
            where TValueMapper : IQueryResultMapper<TValue>, new()
        {
            public GroupByResult() : base("event")
            {
            }
        }

        public sealed class ScanResult<TValue, TValueMapper>
            : WithTwoProperties<string?, TValue, TValueMapper, ScanResult<TValue>>
            where TValueMapper : IQueryResultMapper<TValue>, new()
        {
            public ScanResult() : base(nameof(ScanResult<TValue>.SegmentId), "events", static (id, value) => new(id, value))
            {
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

        public sealed class SourceColumns<TSelf> : Atom<TSelf>
        {
            private static readonly string[] propertyNames = typeof(TSelf)
                .GetProperties()
                .Select(property => property.Name)
                .ToArray();

            // TODO Optimize.
            private protected override TSelf Map(ref Context context)
            {
                var json = context.Deserialize<System.Text.Json.Nodes.JsonObject>(false);
                foreach (var name in propertyNames)
                {
                    var columnName = context.MapperContext.ColumnNames.Get(name);
                    if (columnName != name && json.Remove(columnName, out var value))
                    {
                        if (columnName == "__time")
                        {
                            var unixMs = (long)value!;
                            var t = DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
                            json[name] = t;
                        }
                        else
                            json[name] = value;
                    }
                }

                return json.Deserialize<TSelf>(context.MapperContext.Options)!;
            }
        }
    }

}
