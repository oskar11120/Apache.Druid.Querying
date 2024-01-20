using System;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Internal
{
    internal static class DruidExpressionTextMapper
    {
        public static string Map<TArguments>(Expression<Func<TArguments, string>> factory, IColumnNameMappingProvider columnNameMappings)
        {
            Exception Invalid() => new InvalidOperationException($"{factory} has to return an interpolated string where each argument is a property of {typeof(TArguments)}.");

            if (factory.Body is not MethodCallExpression call ||
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

            return string.Format(template, @params);
        }
    }
}
