using System;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Internal
{
    internal static class DruidExpression
    {
        public static MapResult Map(LambdaExpression factory, IColumnNameMappingProvider columnNameMappings)
        {
            Exception Invalid() => new InvalidOperationException($"{factory} has to return an interpolated string."); // TODO Better error message.
            var body = factory.Body;

            if(body is ConstantExpression constant_ && constant_.Type == typeof(string)) 
                return new((string)constant_.Value!, Array.Empty<string>());

            if (body is not MethodCallExpression call ||
                call.Method.DeclaringType != typeof(string) ||
                call.Method.Name != nameof(string.Format))
                throw Invalid();

            var arguments = call.Arguments;
            if (arguments[0] is not ConstantExpression constant || constant.Type != typeof(string))
                throw Invalid();
            var template = (string)constant.Value!;

            var paramCount = arguments.Count - 1;
            var @params = new string[paramCount];
            for (int i = 0; i < paramCount; i++)
            {
                var member = SelectedProperty.Get(arguments[i + 1]);
                @params[i] = columnNameMappings.GetColumnName(member.SelectedFromType, member.Name);
            }

            return new(string.Format(template, @params), @params);
        }

        public readonly record struct MapResult(string Expression, string[] ColumnNames);
    }
}
