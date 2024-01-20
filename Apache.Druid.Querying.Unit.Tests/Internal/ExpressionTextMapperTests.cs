using Apache.Druid.Querying.Internal;
using FluentAssertions;

namespace Apache.Druid.Querying.Unit.Tests.Internal
{
    internal class ExpressionTextMapperTests
    {
        [Test]
        public void Map_Works()
        {
            var columnNameMap = IColumnNameMappingProvider.ImmutableBuilder.Create<Tests.Message>();
            var result = DruidExpression.Map<Tests.Message>(
                message => $"{message.VariableName} == 42",
                columnNameMap);
            result.Expression.Should().Be("variable == 42");
        }
    }
}
