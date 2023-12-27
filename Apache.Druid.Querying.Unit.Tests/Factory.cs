using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Unit.Tests
{
    internal class Test<TArguments, TAggregations>
    {
        public void Test0(Expression<Func<Factory.IAggregations<TArguments>, TAggregations>> factory)
        {
            var interpreter = new QuerySectionFactoryExpressionInterpreter();
            var calls = interpreter
                .Execute(
                    factory,
                    typeof(Factory.IAggregations<TArguments>),
                    typeof(TArguments),
                    typeof(Factory.IAggregations<>.ColumnSelector<>));
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
                        (scalar, element) => element.Add(scalar.Name, JsonSerializer.SerializeToNode(scalar.Value, scalar.Type)));
                }

                array.Add(element);
            }
        }

        private static string GetColumnName(Expression selectorBody)
        {
            if (selectorBody is UnaryExpression unary)
                return GetColumnName(unary.Operand);

            var expression = (MemberExpression)selectorBody;
            return expression.Member.Name;
        }
    }

    internal class Factory
    {
        public interface IAggregations<TArguments>
        {
            public delegate TColumn ColumnSelector<TColumn>(TArguments arguments);

            public int Count();

            public TColumn Mean<TColumn>(
                ColumnSelector<TColumn> fieldName);

            public TColumn Any<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType? dataType = null);

            public TColumn Sum<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType? dataType = null);
            public TColumn Min<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType? dataType = null);
            public TColumn Max<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType? dataType = null);
            public TColumn Sum<TColumn>(
                ColumnSelector<TColumn> fieldName,
                string expression,
                SimpleDataType? dataType = null);
            public TColumn Min<TColumn>(
                ColumnSelector<TColumn> fieldName,
                string expression,
                SimpleDataType? dataType = null);
            public TColumn Max<TColumn>(
                ColumnSelector<TColumn> fieldName,
                string expression,
                SimpleDataType? dataType = null);

            public TColumn First<TColumn, TTimeColumn>(
                ColumnSelector<TColumn> fieldName,
                ColumnSelector<TTimeColumn> timeColumn,
                SimpleDataType? dataType = null);
            public TColumn Last<TColumn, TTimeColumn>(
                ColumnSelector<TColumn> fieldName,
                ColumnSelector<TTimeColumn> timeColumn,
                SimpleDataType? dataType = null);
            public TColumn First<TColumn>(
               ColumnSelector<TColumn> fieldName,
                SimpleDataType? dataType = null);
            public TColumn Last<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType? dataType = null);
        }
    }
}
