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
            JsonSerializerOptions serializerOptions)
        {
            var calls = SectionFactoryInterpreter.Execute(
                factory,
                typeof(QuerySectionFactory<TElementFactory, TSection>),
                argumentsType);
            var array = new JsonArray();
            foreach (var (member, method, @params) in calls)
            {
                var element = new JsonObject
                {
                    { "name", member },
                    { "type", method }
                };

                foreach (var param in @params)
                {
                    param.Switch(
                        element,
                        (selector, element) => element.Add(selector.Name, selector.MemberName),
                        (scalar, element) => element.Add(scalar.Name, JsonSerializer.SerializeToNode(scalar.Value, scalar.Type, serializerOptions)));
                }

                array.Add(element);
            }

            return array;
        }
    }
}
