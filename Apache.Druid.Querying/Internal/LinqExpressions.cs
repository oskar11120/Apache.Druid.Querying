using System.Linq.Expressions;
using System.Reflection;
using System;
using Apache.Druid.Querying.Internal.Sections;
using System.Collections.Generic;
using System.Linq;

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

        public static IEnumerable<(string PropertyName, Expression AssignedValue)> GetPropertyAssignments<TArgs>(
            this Expression initOrNewExpression,
            TArgs arguments,
            Func<TArgs, string, InvalidOperationException> onUnexpected)
        {
            InvalidOperationException NotCtorNorInit()
                => onUnexpected(arguments, $"Expected {initOrNewExpression} to be a constructor or an intializer call.");
            var init = initOrNewExpression as MemberInitExpression;
            var @new = init is null ?
                initOrNewExpression as NewExpression ?? throw NotCtorNorInit() :
                init.NewExpression;

            if (init is not null)
            {
                foreach (var binding in init.Bindings)
                {
                    var assigment = binding as MemberAssignment ?? throw NotCtorNorInit();
                    var name = assigment.Member.Name;
                    yield return (name, assigment.Expression);
                }
            }

            var propertyNames = @new
                .Type
                .GetProperties()
                .Select(property => property.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            foreach (var (argument, parameter) in @new.Arguments.Zip(@new.Constructor?.GetParameters() ?? Array.Empty<ParameterInfo>()))
            {
                if (!propertyNames.TryGetValue(parameter.Name!, out var name))
                    throw onUnexpected(
                        arguments,
                        $" where all constructor {nameof(parameter)} names have matching property names," +
                        $" which is not the case for {nameof(parameter)}: {parameter.Name}");
                yield return (name, argument);
            }
        }
    }

    public readonly record struct SelectedProperty(Type Type, string Name, Type SelectedFromType)
    {
        public static bool TryGet(Expression selectorBody, out SelectedProperty result)
        {
            selectorBody = selectorBody.UnwrapUnary();
            if (selectorBody is not MemberExpression expression || expression.Member is not PropertyInfo property)
            {
                result = default;
                return false;
            }

            var name = expression.Member.Name;
            var selectedFromExpression = expression.Expression?.UnwrapUnary();
            result = new(property.PropertyType, name, selectedFromExpression?.Type ?? property.DeclaringType!);
            return true;
        }

        public static SelectedProperty Get(Expression selectorBody)
            => TryGet(selectorBody, out var result) ?
            result :
            throw new InvalidOperationException($"{selectorBody} is not a property selector.");
    }
}
