using System.Linq.Expressions;
using System.Reflection;
using System;
using Apache.Druid.Querying.Internal.Sections;

namespace Apache.Druid.Querying.Internal
{
    internal static class ExpresionExtensions
    {
        public static Expression UnwrapUnary(this Expression expression)
            => expression is UnaryExpression unary ? UnwrapUnary(unary.Operand) : expression;

        public static Expression EvaluateCondition(this ConditionalExpression ternary)
        {
            bool result;
            try
            {
                result = (bool)ternary.Test.GetValue()!;
            }
            catch (Exception exception)
            {
                throw new InvalidOperationException($"Could not evaluate condition of ternary expression: {ternary}", exception);
            }

            return result ? ternary.IfTrue : ternary.IfFalse;
        }
    }

    internal readonly record struct SelectedProperty(Type Type, string Name, Type SelectedFromType)
    {
        public static SelectedProperty Get(Expression selectorBody)
        {
            selectorBody = selectorBody.UnwrapUnary();
            var expression = (MemberExpression)selectorBody;
            var name = expression.Member.Name;
            var property = expression.Member as PropertyInfo 
                ?? throw new InvalidOperationException($"{selectorBody} is not a property selector.");
            var selectedFromExpression = expression.Expression?.UnwrapUnary();
            return new(property.PropertyType, name, selectedFromExpression?.Type ?? property.DeclaringType!);
        }
    }
}
