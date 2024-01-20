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
            var result = DruidExpressionTextMapper.Map<Tests.Message>(
                message => $"{message.VariableName} == 42",
                columnNameMap);
            result.Should().Be("variable == 42");
        }
    }
}
