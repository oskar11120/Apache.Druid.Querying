﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Internal.Sections
{
    public static class SectionFactoryJsonMapper
    {
        public static JsonNode Map<TArguments>(
            IReadOnlyList<ElementFactoryCall> calls,
            SectionAtomicity atomicity,
            QueryToJsonMappingContext context,
            Options options)
        {
            void MapCallParam(ElementFactoryCall.Parameter.Any param, string? callResultMemberName, JsonObject result)
                => param.Switch(
                result,
                (selector, result) => result.Add(selector.Name, context.ColumnNames.GetColumnName(selector.SelectedFromType, selector.SelectedName)),
                (scalar, result) =>
                {
                    if (options.SkipScalarParameter?.Invoke(scalar) is true)
                        return;

                    scalar = options.ReplaceScalarParameter?.Invoke(scalar) ?? scalar;
                    var serializerOptions = options.SerializeScalarParameterWithDataSerializer?.Invoke(scalar) is true ?
                        context.DataSerializerOptions : context.QuerySerializerOptions;
                    result.Add(
                        scalar.Name,
                        JsonSerializer.SerializeToNode(scalar.Value, scalar.Type, serializerOptions));
                },
                (nested, result) => result.Add(nested.Name, nested.Single ? 
                    MapCall(nested.Calls.Single(), mapSectionColumnName: true, callResultMemberName) : 
                    Map(nested.Calls, forceSingle: false, mapSectionColumnName: false, callResultMemberName)),
                (expression, result) =>
                {
                    if (expression.Value is null)
                        return;

                    var (value, columnNames) = DruidExpression.Map(expression.Value, context.ColumnNames, context.DataSerializerOptions);
                    result.Add(expression.Name, value);
                    
                    if (options.ExpressionColumnNamesKey is string existing)
                    {
                        if (result.Remove(existing, out var node))
                        {
                            var existingColumnNames = node.Deserialize<IEnumerable<string>>()!;
                            columnNames = existingColumnNames.Concat(columnNames).Distinct().ToArray();
                        }
                        result.Add(existing, JsonSerializer.SerializeToNode(columnNames, context.QuerySerializerOptions));
                    }
                },
                (filterFactory, result) =>
                {
                    var factory = (Func<QueryElementFactory<TArguments>.Filter, IFilter>)filterFactory.Value;
                    var filter = factory.Invoke(new(context));
                    result.Add(filterFactory.Name, JsonSerializer.SerializeToNode(filter, context.QuerySerializerOptions));
                });

            JsonObject MapCall(ElementFactoryCall call, bool mapSectionColumnName, string? parentResultMemberName)
            {
                var (member, method, @params) = call;
                var result = new JsonObject
                {
                    { "type", options!.MapType?.Invoke(call) ?? method.ToCamelCase() }
                };

                var resultMemberName = atomicity.Atomic ? atomicity.ColumnNameIfAtomic : (member ?? parentResultMemberName);
                if (mapSectionColumnName)
                    result.Add(options.SectionColumnNameKey, resultMemberName);
                foreach (var param in @params)
                    MapCallParam(param, resultMemberName, result);
                return result;
            }

            JsonNode Map(IReadOnlyCollection<ElementFactoryCall> calls, bool forceSingle, bool mapSectionColumnName, string? parentResultMemberName)
            {
                static bool IsNone(ElementFactoryCall call) =>
                    call.MethodName is nameof(QueryElementFactory<TArguments>.INone.None) &&
                    call.Parameters.Count is 0;

                if (forceSingle)
                {
                    calls = calls.Where(call => !IsNone(call)).ToArray();
                    return calls.Count is 1 ?
                        MapCall(calls.Single(), mapSectionColumnName, parentResultMemberName) :
                        throw new InvalidOperationException($"Expected single {nameof(ElementFactoryCall)} but got {calls.Count}.")
                        { Data = { [nameof(calls)] = calls } };
                }

                var array = new JsonArray();
                foreach (var call in calls)
                    if (!IsNone(call))
                        array.Add(MapCall(call, mapSectionColumnName, parentResultMemberName));
                return array;
            }

            return Map(calls, options.ForceSingle, mapSectionColumnName: true, parentResultMemberName: null);
        }

        public sealed record Options(
            Func<ElementFactoryCall, string>? MapType = null,
            Func<ElementFactoryCall.Parameter.Scalar, bool>? SkipScalarParameter = null,
            Func<ElementFactoryCall.Parameter.Scalar, ElementFactoryCall.Parameter.Scalar>? ReplaceScalarParameter = null,
            Func<ElementFactoryCall.Parameter.Scalar, bool>? SerializeScalarParameterWithDataSerializer = null,
            string SectionColumnNameKey = "name",
            bool ForceSingle = false,
            string? ExpressionColumnNamesKey = null)
        {
            public static readonly Options Default = new();
        }
    }
}
