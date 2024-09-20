using Apache.Druid.Querying.Internal.Sections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;

namespace Apache.Druid.Querying.Internal
{
    internal static class DruidExpression
    {
        public static MapResult Map(
            LambdaExpression factory,
            PropertyColumnNameMapping.IProvider columnNameMappings,
            JsonSerializerOptions constantSerializerOptions)
            => Map(factory.Body, columnNameMappings, constantSerializerOptions);

        private static MapResult Map(
            Expression expression,
            PropertyColumnNameMapping.IProvider columnNameMappings,
            JsonSerializerOptions constantSerializerOptions)
        {
            InvalidOperationException Invalid(string reason, Exception? inner = null)
                => new($"Invalid Druid expression: {expression}. {reason}.", inner);

            string MapValue(object? value) => value is string text ? 
                text : 
                JsonSerializer.Serialize(value, options: constantSerializerOptions);

            if (expression is ConstantExpression constant_)
                return new(MapValue(constant_.Value), Array.Empty<string>());

            if (expression is BinaryExpression binary && binary.NodeType is ExpressionType.Add)
            {
                var left = Map(binary.Left, columnNameMappings, constantSerializerOptions);
                var right = Map(binary.Right, columnNameMappings, constantSerializerOptions);
                return new(left.Expression + right.Expression, left.ColumnNames.Concat(right.ColumnNames).ToArray());
            }

            if (expression is not MethodCallExpression call ||
                call.Method.DeclaringType != typeof(string) ||
                call.Method.Name != nameof(string.Format))
                throw Invalid($"{expression} is not an interpolated string");

            IEnumerable<string> Map_(Expression param)
            {
                param = param.UnwrapUnary();
                if (param is NewArrayExpression array)
                    return array.Expressions.SelectMany(Map_);

                if (param is ConditionalExpression ternary)
                    return Map_(ternary.EvaluateCondition());

                if (SelectedProperty.TryGet(param, out var property))
                {
                    const char prefixSuffix = '"';
                    var @string = prefixSuffix + columnNameMappings.GetColumnName(property.SelectedFromType, property.Name) + prefixSuffix;
                    return new[] { @string };
                }

                try
                {
                    var value = param.GetValue();
                    return new[] { MapValue(value) };
                }
                catch (Exception exception)
                {
                    throw Invalid($"Error evaluating expression: {param}.", exception);
                }
            }

            var arguments = call.Arguments;
            if (arguments[0] is not ConstantExpression constant || constant.Type != typeof(string))
                throw Invalid($"{arguments[0]} in not {typeof(string).Name}.");
            var template = (string)constant.Value!;

            if (arguments.Count is 1)
                return new(template, Array.Empty<string>());
            var paramStrings = arguments
                .Skip(1)
                .SelectMany(Map_)
                .ToArray();
            return new(string.Format(template, paramStrings), paramStrings);
        }

        public readonly record struct MapResult(string Expression, string[] ColumnNames);
    }
}
