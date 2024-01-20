using System.Linq.Expressions;
using System.Reflection;
using System;

namespace Apache.Druid.Querying.Internal
{
    internal static class ExpresionExtensions
    {
        public static Expression UnwrapUnary(this Expression expression)
            => expression is UnaryExpression unary ? UnwrapUnary(unary.Operand) : expression;
    }

    internal readonly record struct SelectedProperty(Type Type, string Name, Type SelectedFromType)
    {
        public static SelectedProperty Get(Expression selectorBody)
        {
            selectorBody = selectorBody.UnwrapUnary();
            var expression = (MemberExpression)selectorBody;
            var name = expression.Member.Name;
            var property = expression.Member as PropertyInfo ?? throw new InvalidOperationException();
            return new(property.PropertyType, name, property.DeclaringType!);
        }
    }
}
