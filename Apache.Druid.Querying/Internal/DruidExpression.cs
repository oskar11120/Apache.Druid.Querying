using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Internal
{
    internal static class DruidExpression
    {
        private const char columnPrefixSuffix = '"';

        public static MapResult Map(LambdaExpression factory, IColumnNameMappingProvider columnNameMappings)
            => Map(factory.Body, columnNameMappings);

        private static MapResult Map(Expression expression, IColumnNameMappingProvider columnNameMappings)
        {
            Exception Invalid(string reason) => new InvalidOperationException($"Invalid Druid expression: {expression}. {reason}.");

            if (expression is ConstantExpression constant_ && constant_.Type == typeof(string))
                return new((string)constant_.Value!, Array.Empty<string>());

            if (expression is BinaryExpression binary && binary.NodeType is ExpressionType.Add)
            {
                var left = Map(binary.Left, columnNameMappings);
                var right = Map(binary.Right, columnNameMappings);
                return new(left.Expression + right.Expression, left.ColumnNames.Concat(right.ColumnNames).ToArray());
            }

            if (expression is not MethodCallExpression call ||
                call.Method.DeclaringType != typeof(string) ||
                call.Method.Name != nameof(string.Format))
                throw Invalid($"{expression} is not an interpolated string");

            var arguments = call.Arguments;
            if (arguments[0] is not ConstantExpression constant || constant.Type != typeof(string))
                throw Invalid($"{arguments[0]} in not {typeof(string).Name}");
            var template = (string)constant.Value!;

            if (arguments.Count is 1)
                return new(template, Array.Empty<string>());

            var @paramsExpression = arguments[1];
            var @params = @paramsExpression is NewArrayExpression array ?
                array.Expressions :
                new[] { paramsExpression } as IReadOnlyList<Expression>;
            var paramStrings = new string[@params.Count];
            for (int i = 0; i < @params.Count; i++)
            {
                var @param = @params[i];
                var member = SelectedProperty.Get(@param);
                var @string = columnPrefixSuffix + columnNameMappings.GetColumnName(member.SelectedFromType, member.Name) + columnPrefixSuffix;
                paramStrings[i] = @string;
            }

            return new(string.Format(template, paramStrings), paramStrings);
        }

        public readonly record struct MapResult(string Expression, string[] ColumnNames);
    }
}
