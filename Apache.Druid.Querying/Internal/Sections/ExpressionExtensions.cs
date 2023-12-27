using System.Reflection;
using System.Linq.Expressions;
using System;
using System.Linq;

namespace Apache.Druid.Querying.Internal.QuerySectionFactory
{
    internal static class ExpressionExtensions
    {
        public static object? GetValue(this Expression expression)
            => expression switch
            {
                ConstantExpression constant => constant.Value,
                MemberExpression member => GetValue(member),
                MethodCallExpression call => GetValue(call),
                _ => GetValueUsingCompile(expression)
            };

        private static object? GetValue(MemberExpression expression)
        {
            var value = expression.Expression!.GetValue();
            var member = expression.Member;
            switch (member)
            {
                case FieldInfo field:
                    return field.GetValue(value);
                case PropertyInfo property:
                    try
                    {
                        return property.GetValue(value);
                    }
                    catch (TargetInvocationException e)
                    {
                        throw e.InnerException!;
                    }
                default:
                    throw new InvalidOperationException("Unknown member type: " + member.GetType());
            }
        }

        private static object? GetValue(MethodCallExpression expression)
        {
            var arguments = expression
                .Arguments
                .Select(GetValue)
                .ToArray();
            var @object = expression.Object!.GetValue();

            try
            {
                return expression.Method.Invoke(@object, arguments);
            }
            catch (TargetInvocationException e)
            {
                throw e.InnerException!;
            }
        }

        private static object? GetValueUsingCompile(Expression expression)
            => Expression.Lambda(expression).Compile().DynamicInvoke();
    }
}