﻿using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Internal.Sections
{
    internal class SectionFactoryJsonMapper
    {
        public static JsonArray Map<TElementFactory, TSection>(
            Expression<QuerySectionFactory<TElementFactory, TSection>> factory,
            Type argumentsType,
            string sectionKey,
            JsonSerializerOptions serializerOptions,
            IArgumentColumnNameProvider columnNames,
            CustomMappings? customMappings)
        {
            JsonArray Map(IEnumerable<ElementFactoryCall> calls)
            {
                var array = new JsonArray();
                foreach (var call in calls)
                {
                    var (member, method, @params) = call;
                    var element = new JsonObject
                    {
                        { customMappings?.SectionColumnNameKey ?? "name", member ?? sectionKey },
                        { "type", customMappings?.MapType?.Invoke(call) ?? method }
                    };

                    foreach (var param in @params)
                    {
                        param.Switch(
                            element,
                            (selector, element) => element.Add(selector.Name, columnNames.Get(selector.MemberName)),
                            (scalar, element) =>
                            {
                                if (customMappings?.SkipScalarParameter?.Invoke(scalar) is true)
                                    return;

                                scalar = customMappings?.ReplaceScalarParameter?.Invoke(scalar) ?? scalar;
                                element.Add(
                                    scalar.Name,
                                    JsonSerializer.SerializeToNode(scalar.Value, scalar.Type, serializerOptions));
                            },
                            (nested, element) => element.Add(nested.Name, Map(nested.Calls)));
                    }

                    array.Add(element);
                }

                return array;
            }

            var calls = SectionFactoryInterpreter.Execute(
                factory,
                typeof(QuerySectionFactory<TElementFactory, TSection>),
                argumentsType);
            return Map(calls);
        }

        public sealed record CustomMappings(
            Func<ElementFactoryCall, string>? MapType = null,
            Func<ElementFactoryCall.Parameter.Scalar, bool>? SkipScalarParameter = null,
            Func<ElementFactoryCall.Parameter.Scalar, ElementFactoryCall.Parameter.Scalar>? ReplaceScalarParameter = null,
            string? SectionColumnNameKey = null);
    }
}