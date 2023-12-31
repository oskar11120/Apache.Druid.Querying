using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Internal.Sections
{
    internal class SectionFactoryJsonMapper
    {
        public static JsonNode Map(
            IReadOnlyList<ElementFactoryCall> calls,
            SectionAtomicity atomicity,
            JsonSerializerOptions serializerOptions,
            IArgumentColumnNameProvider columnNames,
            Options options)
        {
            void MapCallParam(ElementFactoryCall.Parameter.Any param, JsonObject result)
                => param.Switch(
                    result,
                    (selector, result) => result.Add(selector.Name, columnNames.Get(selector.MemberName)),
                    (scalar, result) =>
                    {
                        if (options.SkipScalarParameter?.Invoke(scalar) is true)
                            return;

                        scalar = options.ReplaceScalarParameter?.Invoke(scalar) ?? scalar;
                        result.Add(
                            scalar.Name,
                            JsonSerializer.SerializeToNode(scalar.Value, scalar.Type, serializerOptions));
                    },
                    (nested, element) => element.Add(nested.Name, Map(nested.Calls, true)));

            JsonObject MapCall(ElementFactoryCall call, bool nested)
            {
                var (member, method, @params) = call;
                var result = new JsonObject
                {
                    { "type", options!.MapType?.Invoke(call) ?? method.ToCamelCase() }
                };

                if (!nested)
                    result.Add(options.SectionColumnNameKey, atomicity.Atomic ? atomicity.ColumnNameIfAtomic : member);
                foreach (var param in @params)
                    MapCallParam(param, result);
                return result;
            }

            JsonNode Map(IReadOnlyCollection<ElementFactoryCall> calls, bool nested)
            {
                if (options.ForceSingle || atomicity.Atomic)
                {
                    return calls.Count == 1 ?
                        MapCall(calls.Single(), nested) :
                        throw new InvalidOperationException();
                }

                var array = new JsonArray();
                foreach (var call in calls)
                    array.Add(MapCall(call, nested));
                return array;
            }

            return Map(calls, false);
        }

        public sealed record Options(
            Func<ElementFactoryCall, string>? MapType = null,
            Func<ElementFactoryCall.Parameter.Scalar, bool>? SkipScalarParameter = null,
            Func<ElementFactoryCall.Parameter.Scalar, ElementFactoryCall.Parameter.Scalar>? ReplaceScalarParameter = null,
            string SectionColumnNameKey = "name",
            bool ForceSingle = false)
        {
            public static readonly Options Default = new();
        }
    }
}
