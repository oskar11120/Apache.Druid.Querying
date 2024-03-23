using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Internal.Sections
{
    internal class SectionFactoryJsonMapper
    {
        public static JsonNode Map<TArguments>(
            IReadOnlyList<ElementFactoryCall> calls,
            SectionAtomicity atomicity,
            JsonSerializerOptions serializerOptions,
            IColumnNameMappingProvider columnNameMappings,
            Options options)
        {
            void MapCallParam(ElementFactoryCall.Parameter.Any param, JsonObject result)
                => param.Switch(
                result,
                (selector, result) => result.Add(selector.Name, columnNameMappings.GetColumnName(selector.SelectedFromType, selector.SelectedName)),
                (scalar, result) =>
                {
                    if (options.SkipScalarParameter?.Invoke(scalar) is true)
                        return;

                    scalar = options.ReplaceScalarParameter?.Invoke(scalar) ?? scalar;
                    result.Add(
                        scalar.Name,
                        JsonSerializer.SerializeToNode(scalar.Value, scalar.Type, serializerOptions));
                },
                (nested, result) => result.Add(nested.Name, nested.Single ? MapCall(nested.Calls.Single(), true) : Map(nested.Calls, true)),
                (expression, result) =>
                {
                    if (expression.Value is null)
                        return;

                    var (value, columnNames) = DruidExpression.Map(expression.Value, columnNameMappings);
                    result.Add(expression.Name, value);
                    if (options.ExpressionColumnNamesKey is string existing)
                        result.Add(existing, JsonSerializer.SerializeToNode(columnNames, serializerOptions));
                },
                (filterFactory, result) =>
                {
                    var factory = (Func<QueryElementFactory<TArguments>.Filter, IFilter>)filterFactory.Value;
                    var filter = factory.Invoke(new(columnNameMappings));
                    result.Add(filterFactory.Name, JsonSerializer.SerializeToNode(filter, serializerOptions));
                });

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
                static bool IsNone(ElementFactoryCall call) =>
                    call.ResultMemberName is null &&
                    call.MethodName is "None" &&
                    call.Parameters.Count is 0;

                if (!nested && options.ForceSingle)
                {
                    calls = calls.Where(call => !IsNone(call)).ToArray();
                    return calls.Count is 1 ?
                        MapCall(calls.Single(), nested) :
                        throw new InvalidOperationException($"Expected single {nameof(ElementFactoryCall)} but got {calls.Count}.")
                        { Data = { [nameof(calls)] = calls } };
                }

                var array = new JsonArray();
                foreach (var call in calls)
                    if (!IsNone(call))
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
            bool ForceSingle = false,
            string? ExpressionColumnNamesKey = null)
        {
            public static readonly Options Default = new();
        }
    }
}
