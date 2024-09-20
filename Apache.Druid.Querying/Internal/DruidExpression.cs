using System;
using System.Collections.Generic;
using System.Globalization;
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

            if (expression is ConstantExpression constant_)
                return new(
                    constant_.Value switch
                    {
                        string text => text,
                        var any => JsonSerializer.Serialize(any, options: constantSerializerOptions)
                    },
                    Array.Empty<string>());

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

                if(param is ConstantExpression constant)
                    return new[] { constant.Value?.ToString() ?? string.Empty };

                if (param is ConditionalExpression ternary)
                    return Map_(ternary.EvaluateCondition());

                SelectedProperty member;
                try
                {
                    member = SelectedProperty.Get(@param);
                }
                catch (Exception exception)
                {
                    throw Invalid(exception.Message, exception);
                }

                const char prefixSuffix = '"';
                var @string = prefixSuffix + columnNameMappings.GetColumnName(member.SelectedFromType, member.Name) + prefixSuffix;
                return new[] { @string };
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
