using Apache.Druid.Querying.Internal.Json;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Apache.Druid.Querying.Internal
{
    internal static class QueryResultElement
    {
        public interface IDeserializer<TElement>
        {
            public TElement Deserialize(ref DeserializerContext context);
        }

        public ref struct DeserializerContext
        {
            [ThreadStatic]
            private static Dictionary<Type, object?>? deserializers;

            private static readonly byte[] timestampPropertyNameBytes = Encoding.UTF8.GetBytes("__time");
            private static readonly byte[] comaBytes = Encoding.UTF8.GetBytes(",");
            private readonly int spanningBytes;
            private readonly int trimBytes;
            private long deserializeConsumedBytes = 0;
            private readonly QueryResultMapperContext mapperContext;

            public DeserializerContext(QueryResultMapperContext mapperContext, int spanningBytes)
            {
                this.mapperContext = mapperContext;
                this.spanningBytes = spanningBytes;

                var startWithComa = mapperContext.Json.UnreadPartOfBufferStartsWith(comaBytes);
                trimBytes = startWithComa ? comaBytes.Length : 0;
            }

            private static Utf8JsonReader WrapInReader(ReadOnlySpan<byte> slice)
                => new(slice, false, default);

            public TElementOrElementPart Deserialize<TElementOrElementPart>(bool checkForAtomicity = true)
            {
                if (GetDeserializer<TElementOrElementPart>() is IDeserializer<TElementOrElementPart> existing)
                    return existing.Deserialize(ref this);

                if (mapperContext.ColumnNameMappings.Get<TElementOrElementPart>() is var mappings and { Count: > 0 })
                    return DeserializeApplyMappings<TElementOrElementPart>(mappings);

                var (json, options, atomicity, _) = mapperContext;
                SectionAtomicity atomicity_;
                if (checkForAtomicity && (atomicity_ = atomicity.Get<TElementOrElementPart>()).Atomic)
                    return DeserializeProperty<TElementOrElementPart>(atomicity_.ColumnNameIfAtomicUtf8);

                var slice = json.GetSliceOfBuffer(spanningBytes, trimBytes);
                deserializeConsumedBytes = slice.Length;
                var result = JsonSerializer.Deserialize<TElementOrElementPart>(slice, options)!;
                return result;
            }

            public TProperty DeserializeProperty<TProperty>(ReadOnlySpan<byte> propertyNameUtf8)
            {
                var (json, options, _, _) = mapperContext;
                var @object = json.GetSliceOfBuffer(spanningBytes, trimBytes);
                deserializeConsumedBytes = @object.Length;
                var reader = WrapInReader(@object);
                reader.ReadToProperty(propertyNameUtf8);
                var start = (int)reader.BytesConsumed;
                var propertyDepth = reader.CurrentDepth;
                do
                    reader.Read();
                while (reader.CurrentDepth < propertyDepth);
                var property = @object[start..(int)reader.BytesConsumed];

                // TODO More TProperty types
                if (typeof(TProperty) == typeof(DateTimeOffset) && propertyNameUtf8 == timestampPropertyNameBytes)
                {
                    var unixMs = JsonSerializer.Deserialize<long>(property, options);
                    var t = DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
                    return Unsafe.As<DateTimeOffset, TProperty>(ref t);
                }

                return JsonSerializer.Deserialize<TProperty>(property, options)!;
            }

            public DateTimeOffset DeserializeTimeProperty()
                 => DeserializeProperty<DateTimeOffset>(timestampPropertyNameBytes);

            public readonly void UpdateState()
            {
                var json = mapperContext.Json;
                var reader = json.GetReader();
                while (reader.BytesConsumed < deserializeConsumedBytes + trimBytes)
                    reader.Read();
                json.UpdateState(reader);
            }

            private T DeserializeApplyMappings<T>(IReadOnlyList<PropertyColumnNameMapping> mappings)
            {
                var json = Deserialize<System.Text.Json.Nodes.JsonObject>(false);
                foreach (var (property, column) in mappings)
                {
                    if (json.Remove(column, out var value))
                    {
                        if (column == "__time")
                        {
                            var unixMs = (long)value!;
                            var t = DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
                            json[property] = t;
                        }
                        else
                            json[property] = value;
                    }
                }

                return json.Deserialize<T>(mapperContext.Options)!;
            }

            private static IDeserializer<TElement>? GetDeserializer<TElement>()
            {
                var elementType = typeof(TElement);
                deserializers ??= new();
                if (deserializers.TryGetValue(elementType, out var deserializer))
                {
                    return deserializer as IDeserializer<TElement>;
                }

                var interfaceType = typeof(IDeserializer<TElement>);
                var interfaceOpenType = interfaceType.GetGenericTypeDefinition();
                var nestedTypes = elementType.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic);
                var some = Array.Find(nestedTypes, type => type.Name == "Deserializer");
                var result = some switch
                {
                    { ContainsGenericParameters: true } => Activator.CreateInstance(some.MakeGenericType(elementType.GetGenericArguments())),
                    { ContainsGenericParameters: false } => Activator.CreateInstance(some),
                    _ => null
                }
                as IDeserializer<TElement>;

                deserializers.Add(elementType, result);
                return result;
            }
        }
    }

    public static partial class QueryResultMapper
    {
        public abstract class TwoPropertyObject<TFirst, TSecond, TSecondMapper, TResult> :
            IQueryResultMapper<TResult>
            where TSecondMapper : IQueryResultMapper<TSecond>, new()
        {
            private static readonly IQueryResultMapper<TSecond> mapper = new TSecondMapper();
            private readonly (byte[] First, byte[] Second) names;
            private readonly Func<TFirst, TSecond, TResult> create;

            public TwoPropertyObject(string firstName, string secondName, Func<TFirst, TSecond, TResult> create)
            {
                names = (ToJson(firstName), ToJson(secondName));
                this.create = create;
            }

            async IAsyncEnumerable<TResult> IQueryResultMapper<TResult>.Map(
                QueryResultMapperContext context, [EnumeratorCancellation] CancellationToken token)
            {
                var json = context.Json;
                TFirst first;
                while (!json.ReadToPropertyValue(names.First, out first))
                    await json.AdvanceAsync(token);
                while (!json.ReadToProperty(names.Second))
                    await json.AdvanceAsync(token);

                var results = mapper.Map(context, token);
                await foreach (var result in results)
                    yield return create(first, result);

                while (!json.ReadToToken(JsonTokenType.EndObject))
                    await json.AdvanceAsync(token);
            }

            private static byte[] ToJson(string propertyName) =>
                Encoding.UTF8.GetBytes(propertyName.ToCamelCase());
        }

        public class Array<TElement, TElementMapper> :
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

                while (!json.ReadToToken(JsonTokenType.StartArray))
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

        // "Element" are objects small enough that whole their data can be fit into buffer. 
        public sealed class Element<TSelf> : IQueryResultMapper<TSelf>
        {
            async IAsyncEnumerable<TSelf> IQueryResultMapper<TSelf>.Map(
                QueryResultMapperContext context, [EnumeratorCancellation] CancellationToken token)
            {
                var json = context.Json;
                var bytes = await EnsureWholeInBufferAndGetSpanningBytesAsync(json, token);
                TSelf Map_()
                {
                    var context_ = new QueryResultElement.DeserializerContext(context, bytes);
                    var result = context_.Deserialize<TSelf>(checkForAtomicity: false); // TODO verify atomicity
                    context_.UpdateState();
                    return result;
                }

                yield return Map_();
            }

            private static async ValueTask<int> EnsureWholeInBufferAndGetSpanningBytesAsync(JsonStreamReader json, CancellationToken token)
            {
                long bytesConsumed;
                while (!json.ReadToTokenTypeAtNextTokenDepth(JsonTokenType.EndObject, out bytesConsumed, false))
                    await json.AdvanceAsync(token);
                return (int)bytesConsumed;
            }
        }
    }
}
