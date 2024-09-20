using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Json;
using FluentAssertions;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Tests.Unit.Internal
{
    internal class DruidExpressionMapperShouldHadle
    {
        private static readonly PropertyColumnNameMapping.ImmutableBuilder columnMappings
            = PropertyColumnNameMapping.ImmutableBuilder.Create<QueryShould_MapToRightJson.IotMeasurement>();
        private static string Map(Expression<QueryElementFactory<QueryShould_MapToRightJson.IotMeasurement>.DruidExpression> factory)
            => DruidExpression.Map(factory, columnMappings, DefaultSerializerOptions.Data).Expression;

        [Test]
        public void OneParameter()
        {
            string result = Map(message => $"{message.SignalName} == 42");
            result.Should().Be("\"signal\" == 42");
        }

        [Test]
        public void TwoParameters()
        {
            string result = Map(message => $"{message.SignalName} == 42 && {message.Value} > 0");
            result.Should().Be("\"signal\" == 42 && \"value\" > 0");
        }

        [Test]
        public void NoParameters()
        {
            string result = Map(message => $"\"signal\" == 42");
            result.Should().Be("\"signal\" == 42");
        }

        [Test]
        public void Concatenation()
        {
            var result = Map(message =>
                $"{message.SignalName} == 42 && " +
                $"{message.Value} > 0 && " + 
                $"{message.Value} < 1000");
            result.Should().Be("\"signal\" == 42 && \"value\" > 0 && \"value\" < 1000");
        }

        [Test]
        public void TernaryOperators()
        {
            int value = 1;
            string result = Map(message => $"{message.SignalName} == {(value == 1 ? 42 : 41)}");
            result.Should().Be("\"signal\" == 42");
        }
    }
}
