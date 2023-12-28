using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System;
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
            var calls = SectionFactoryInterpreter.Execute(
                factory,
                typeof(QuerySectionFactory<TElementFactory, TSection>),
                argumentsType);
            var array = new JsonArray();
            foreach (var call in calls)
            {
                var (member, method, @params) = call;
                var element = new JsonObject
                {
                    { "name", member ?? sectionKey },
                    { "type", customMappings?.MapType?.Invoke(call) ?? method }
                };

                foreach (var param in @params)
                {
                    param.Switch(
                        element,
                        (selector, element) => element.Add(selector.Name, columnNames.Get(selector.MemberName)),
                        (scalar, element) =>
                        {
                            if (customMappings?.SkipScalarParameter?.Invoke(scalar) is false)
                                element.Add(
                                    scalar.Name,
                                    JsonSerializer.SerializeToNode(scalar.Value, scalar.Type, serializerOptions));
                        });
                }

                array.Add(element);
            }

            return array;
        }

        public sealed record CustomMappings(
            Func<ElementFactoryCall, string>? MapType = null,
            Func<ElementFactoryCall.Parameter.Scalar, bool>? SkipScalarParameter = null);
    }
}
