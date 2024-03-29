﻿using Apache.Druid.Querying.Internal.Json;
using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
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
        public delegate TElement Deserializer<TElement>(ref DeserializerContext context);

        public ref struct DeserializerContext
        {
            private static readonly ConcurrentDictionary<Type, object?> deserializers = new();
            private static readonly ConcurrentDictionary<Type, bool> hasAnyColumnNamesVaryingFromPropertyNames = new();

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

            public TElementOrElementPart Deserialize<TElementOrElementPart>()
            {
                if (GetDeserializer<TElementOrElementPart>() is Deserializer<TElementOrElementPart> existing)
                    return existing(ref this);

                if (GetMappingsIfAnyColumnNamesVaryingFromPropertyNames<TElementOrElementPart>() is var mappings and { Count: > 0 })
                    return DeserializeApplyMappings<TElementOrElementPart>(mappings);

                if (atomicity.TryGet<TElementOrElementPart>() is SectionAtomicity { Atomic: true, ColumnNameIfAtomicUtf8: var atomicColumnName })
                    return DeserializeProperty<TElementOrElementPart>(atomicColumnName);

                var result = JsonSerializer.Deserialize<TElementOrElementPart>(json, serializerOptions)!;
                return result;
            }

            public readonly TProperty DeserializeProperty<TProperty>(ReadOnlySpan<byte> propertyNameUtf8)
            {
                var reader = new Utf8JsonReader(json, false, default);
                if (!reader.ReadToProperty(propertyNameUtf8))
                    throw new InvalidOperationException($"Object {ToString(json)} is missing required property {ToString(propertyNameUtf8)}.");
                var left = (int)reader.BytesConsumed;
                reader.Read();
                if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
                {
                    var endToken = reader.TokenType is JsonTokenType.StartObject ? JsonTokenType.EndObject : JsonTokenType.EndArray;
                    reader.ReadToToken(endToken, reader.CurrentDepth);
                }

                var value = json[left..(int)reader.BytesConsumed];
                return JsonSerializer.Deserialize<TProperty>(value, serializerOptions)!;
            }

            private static string ToString(ReadOnlySpan<byte> utf8) => Encoding.UTF8.GetString(utf8);

            private T DeserializeApplyMappings<T>(IReadOnlyList<PropertyColumnNameMapping> mappings)
            {
                var json = Deserialize<System.Text.Json.Nodes.JsonObject>();
                foreach (var (property, column) in mappings)
                    if (json.Remove(column, out var value))
                        json[property] = value;
                return json.Deserialize<T>(serializerOptions)!;
            }

            private static Deserializer<TElement>? GetDeserializer<TElement>()
            {
                var elementType = typeof(TElement);
                if (deserializers.TryGetValue(elementType, out var existing))
                {
                    return existing as Deserializer<TElement>;
                }

                var fields = elementType.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
                var @new = fields
                    .FirstOrDefault(field => field.Name.Equals("Deserializer", StringComparison.OrdinalIgnoreCase))
                    ?.GetValue(null)
                    as Deserializer<TElement>;
                deserializers.TryAdd(elementType, @new);
                return @new;
            }

            private readonly IReadOnlyList<PropertyColumnNameMapping> GetMappingsIfAnyColumnNamesVaryingFromPropertyNames<TElement>()
            {
                if (!hasAnyColumnNamesVaryingFromPropertyNames.TryGetValue(typeof(TElement), out var result)) 
                {
                    result = columnNameMappings.Get<TElement>() is var mappings and { Count: > 0 }
                        && mappings.Any(mapping => mapping.Property != mapping.ColumnName);
                    hasAnyColumnNamesVaryingFromPropertyNames.TryAdd(typeof(TElement), result);
                }

                return result ? columnNameMappings.Get<TElement>() : Array.Empty<PropertyColumnNameMapping>();
            }
        }
    }

    public static partial class QueryResultDeserializer
    {
        public abstract class TwoPropertyObject<TFirst, TSecond, TSecondMapper, TResult> :
            IQueryResultDeserializer<TResult>
            where TSecondMapper : IQueryResultDeserializer<TSecond>, new()
        {
            private readonly (byte[] First, byte[] Second) names;
            private readonly Func<TFirst, TSecond, TResult> create;

            public TwoPropertyObject(string firstName, string secondName, Func<TFirst, TSecond, TResult> create)
            {
                names = (ToJson(firstName), ToJson(secondName));
                this.create = create;
            }

            async IAsyncEnumerable<TResult> IQueryResultDeserializer<TResult>.Deserialize(
                QueryResultDeserializerContext context, [EnumeratorCancellation] CancellationToken token)
            {
                var json = context.Json;
                var first = await json.ReadToPropertyValueAsync<TFirst>(names.First, token);
                await json.ReadToPropertyAsync(names.Second, token);
                var results = Singleton<TSecondMapper>.Value.Deserialize(context, token);
                await foreach (var result in results)
                    yield return create(first, result);
                await json.ReadToTokenAsync(JsonTokenType.EndObject, json.Depth - 1, token);
            }

            private static byte[] ToJson(string propertyName) =>
                Encoding.UTF8.GetBytes(propertyName.ToCamelCase());
        }

        public class Array<TElement, TElementMapper> :
            IQueryResultDeserializer<TElement>
            where TElementMapper : IQueryResultDeserializer<TElement>, new()
        {
            async IAsyncEnumerable<TElement> IQueryResultDeserializer<TElement>.Deserialize(
                QueryResultDeserializerContext context, [EnumeratorCancellation] CancellationToken token)
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

                    var results = Singleton<TElementMapper>.Value.Deserialize(context, token);
                    await foreach (var result in results)
                        yield return result;

                    if (await TrySkipEndArray())
                        yield break;
                }
            }
        }

        // "Elements" are objects small enough that whole their data can be fit into buffer. 
        public sealed class Element<TSelf> : IQueryResultDeserializer<TSelf>
        {
            private static readonly byte[] comaBytes = Encoding.UTF8.GetBytes(",");

            async IAsyncEnumerable<TSelf> IQueryResultDeserializer<TSelf>.Deserialize(
                QueryResultDeserializerContext context, [EnumeratorCancellation] CancellationToken token)
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
                    try
                    {
                        var result = context_.Deserialize<TSelf>();
                        return result;
                    }
                    catch (Exception ex)
                    {
                        ex.Data.Add("thrownWhileDeserializingElement", Encoding.UTF8.GetString(span));
                        throw;
                    }
                }

                yield return Map_();
                await ReadThroghWholeAsync();
            }
        }
    }
}
