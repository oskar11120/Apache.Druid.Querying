using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Internal.Sections
{
    internal class FromExpressionQuerySectionFactory
    {
        public static JsonArray Create<TArguments, TSection>(
            Expression<QuerySectionFactory<TArguments, TSection>> expression,
            JsonSerializerOptions serializerOptions)
        {
            var calls = SectionExpressionInterpreter.Execute(
                expression,
                typeof(QuerySectionFactory<TArguments, TSection>),
                typeof(TArguments));
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
                        (selector, element) => element.Add("fieldName", selector.Name),
                        (scalar, element) => element.Add(scalar.Name, JsonSerializer.SerializeToNode(scalar.Value, scalar.Type, serializerOptions)));
                }

                array.Add(element);
            }

            return array;
        }
    }
}
