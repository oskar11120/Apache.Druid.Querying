using Apache.Druid.Querying.Internal;
using FluentAssertions;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Tests.Unit.Internal
{
    internal class DruidExpressionMapperTests
    {
        [Test]
        public void Map_Works()
        {
            var columnNameMap = IColumnNameMappingProvider.ImmutableBuilder.Create<Tests.Message>();
            string Map(Expression<QueryElementFactory<Tests.Message>.DruidExpression> factory)
                => DruidExpression.Map(factory, columnNameMap).Expression;
            var result = Map(message => $"{message.VariableName} == 42");
            result.Should().Be("variable == 42");
        }
    }
}
