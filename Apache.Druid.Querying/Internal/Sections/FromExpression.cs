using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Internal.Sections
{
    internal class FromExpression
    {
        public static JsonObject Create<TArguments, TSection>(
            Expression<QuerySectionFactory<TArguments, TSection>> expression,
            JsonSerializerOptions serializerOptions)
        {

        }
    }
}
