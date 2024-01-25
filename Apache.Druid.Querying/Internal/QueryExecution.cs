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
            private const string timeColumn = "__time";
            private static readonly byte[] timeColumnUtf8Bytes = Encoding.UTF8.GetBytes(timeColumn);

            private readonly ReadOnlySpan<byte> json;
            private readonly JsonSerializerOptions serializerOptions;
            private readonly IColumnNameMappingProvider columnNameMappings;
            private readonly SectionAtomicity.IProvider atomicity;

            public DeserializerContext(
                ReadOnlySpan<byte> json,
                JsonSerializerOptions serializerOptions,
                IColumnNameMappingProvider columnNameMappings,
                SectionAtomicity.IProvider atomicity)
            {
                this.json = json;
                this.serializerOptions = serializerOptions;
                this.columnNameMappings = columnNameMappings;
                this.atomicity = atomicity;
            }

            public TElementOrElementPart Deserialize<TElementOrElementPart>(bool checkForAtomicity = true)
            {
                if (GetDeserializer<TElementOrElementPart>() is IDeserializer<TElementOrElementPart> existing)
                    return existing.Deserialize(ref this);

                if (columnNameMappings.Get<TElementOrElementPart>() is var mappings and { Count: > 0 })
                    return DeserializeApplyMappings<TElementOrElementPart>(mappings);

                SectionAtomicity atomicity_;
                if (checkForAtomicity && (atomicity_ = atomicity.Get<TElementOrElementPart>()).Atomic)
                    return DeserializeProperty<TElementOrElementPart>(atomicity_.ColumnNameIfAtomicUtf8);

                var result = JsonSerializer.Deserialize<TElementOrElementPart>(json, serializerOptions)!;
                return result;
            }

            private readonly TProperty DeserializePropertyBase<TProperty>(ReadOnlySpan<byte> propertyNameUtf8)
            {
                var reader = new Utf8JsonReader(json, false, default);
                return reader.ReadToPropertyValue(propertyNameUtf8, out TProperty property) ?
                    property :
                    throw new InvalidOperationException($"Object {ToString(json)} is missing required property {ToString(propertyNameUtf8)}.");
            }

            private readonly TProperty DeserializeProperty<TProperty>(ReadOnlySpan<byte> propertyNameUtf8)
            {
                var type = typeof(TProperty);
                var timeRelated = type == typeof(DateTimeOffset) || type == typeof(DateTime);
                if (!timeRelated || !propertyNameUtf8.SequenceEqual(timeColumnUtf8Bytes))
                {
                    return DeserializePropertyBase<TProperty>(propertyNameUtf8);
                }

                var unixMs = DeserializePropertyBase<long>(propertyNameUtf8);
                var t = DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
                if (type == typeof(DateTimeOffset))
                    return Unsafe.As<DateTimeOffset, TProperty>(ref t);

                var dateTime = t.UtcDateTime;
                return Unsafe.As<DateTime, TProperty>(ref dateTime);
            }

            public readonly DateTimeOffset DeserializeTimeProperty()
                 => DeserializeProperty<DateTimeOffset>(timeColumnUtf8Bytes);

            private static string ToString(ReadOnlySpan<byte> utf8) => Encoding.UTF8.GetString(utf8);

            private T DeserializeApplyMappings<T>(IReadOnlyList<PropertyColumnNameMapping> mappings)
            {
                var json = Deserialize<System.Text.Json.Nodes.JsonObject>(false);
                foreach (var (property, column) in mappings)
                {
                    if (json.Remove(column, out var value))
                    {
                        if (column == timeColumn)
                        {
                            var unixMs = (long)value!;
                            var t = DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
                            json[property] = t;
                        }
                        else
                            json[property] = value;
                    }
                }

                return json.Deserialize<T>(serializerOptions)!;
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
                var first = await json.ReadToPropertyValueAsync<TFirst>(names.First, token);
                await json.ReadToPropertyAsync(names.Second, token);
                var results = mapper.Map(context, token);
                await foreach (var result in results)
                    yield return create(first, result);
                await json.ReadToTokenAsync(JsonTokenType.EndObject, token);
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
                async ValueTask<bool> TrySkipEndArray()
                {
                    var next = await json.ReadNextAsync(token, updateState: false);
                    if (next is JsonTokenType.EndArray)
                    {
                        await json.ReadNextAsync(token);
                        return true;
                    }

                    return false;
                }

                await json.ReadToTokenAsync(JsonTokenType.StartArray, token);
                while (true)
                {
                    if (await TrySkipEndArray())
                        yield break;

                    var results = mapper.Map(context, token);
                    await foreach (var result in results)
                        yield return result;

                    if (await TrySkipEndArray())
                        yield break;
                }
            }
        }

        // "Element" are objects small enough that whole their data can be fit into buffer. 
        public sealed class Element<TSelf> : IQueryResultMapper<TSelf>
        {
            private static readonly byte[] comaBytes = Encoding.UTF8.GetBytes(",");

            async IAsyncEnumerable<TSelf> IQueryResultMapper<TSelf>.Map(
                QueryResultMapperContext context, [EnumeratorCancellation] CancellationToken token)
            {
                var (json, options, atomicity, columnNameMappings) = context;
                async ValueTask<int> ReadThroghWholeAsync(bool updateState = true)
                    => (int)await json.ReadToTokenAsync(
                        JsonTokenType.EndObject,
                        json.Depth + (json.TokenType is JsonTokenType.StartArray ? 1 : 0),
                        token,
                        updateState);
                var read = await ReadThroghWholeAsync(updateState: false);
                TSelf Map_()
                {
                    var span = json.GetSpan()[..read];
                    var startsWithComa = comaBytes.AsSpan().SequenceEqual(span[..comaBytes.Length]);
                    span = startsWithComa ? span[comaBytes.Length..] : span;
                    var context_ = new QueryResultElement.DeserializerContext(span, options, columnNameMappings, atomicity);
                    var result = context_.Deserialize<TSelf>(checkForAtomicity: false); // TODO verify atomicity
                    return result;
                }

                yield return Map_();
                await ReadThroghWholeAsync();
            }
        }
    }
}
