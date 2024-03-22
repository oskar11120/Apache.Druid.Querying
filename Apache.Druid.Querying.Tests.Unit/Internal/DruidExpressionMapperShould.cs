using Apache.Druid.Querying.Internal;
using FluentAssertions;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Tests.Unit.Internal
{
    internal class DruidExpressionMapperShould
    {
        [Test]
        public void Work()
        {
            var columnNameMap = IColumnNameMappingProvider.ImmutableBuilder.Create<QueryShould_MapToRightJson.IotMeasurement>();
            string Map(Expression<QueryElementFactory<QueryShould_MapToRightJson.IotMeasurement>.DruidExpression> factory)
                => DruidExpression.Map(factory, columnNameMap).Expression;
            var result = Map(message => $"{message.SignalName} == 42");
            result.Should().Be("\"signal\" == 42");
        }
    }
}
