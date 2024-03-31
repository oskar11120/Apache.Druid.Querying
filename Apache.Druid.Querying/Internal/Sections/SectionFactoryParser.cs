using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Apache.Druid.Querying.Internal.Sections
{
    internal sealed record ElementFactoryCall(
        string? ResultMemberName,
        string MethodName,
        IReadOnlyList<ElementFactoryCall.Parameter.Any> Parameters)
    {
        public Parameter.Scalar? TryGetScalarParameter<TParameter>() => Parameters
            .SingleOrDefault(param => param.Scalar?.Type == typeof(TParameter))
            ?.Scalar;

        public Parameter.ArgumentsMemberSelector? TryGetMemberSelectorParameter(string name) => Parameters
           .SingleOrDefault(param => param.Selector?.Name == name)
           ?.Selector;

        public Parameter.ArgumentsMemberSelector GetMemberSelectorParameter(string name)
            => TryGetMemberSelectorParameter(name) ??
            throw new InvalidOperationException($"Method {MethodName} is missing required parameter {name}.")
            {
                Data = { ["call"] = this }
            };

        public static class Parameter
        {
            public sealed record Any(
                ArgumentsMemberSelector? Selector = null,
                Scalar? Scalar = null,
                Nested? Nested = null,
                DruidExpression? Expression = null,
                FilterFactory? FilterFactory = null)
            {
                public TResult Switch<TResult, TArg>(
                    TArg argument,
                    Func<ArgumentsMemberSelector, TArg, TResult> ifMemberSelector,
                    Func<Scalar, TArg, TResult> ifScalar,
                    Func<Nested, TArg, TResult> ifNested,
                    Func<DruidExpression, TArg, TResult> ifExpression,
                    Func<FilterFactory, TArg, TResult> ifFilterFactory)
                    => (Selector, Scalar, Nested, Expression, FilterFactory) switch
                    {
                        (ArgumentsMemberSelector selector, null, null, null, null) => ifMemberSelector(selector, argument),
                        (null, Scalar scalar, null, null, null) => ifScalar(scalar, argument),
                        (null, null, Nested nested, null, null) => ifNested(nested, argument),
                        (null, null, null, DruidExpression expression, null) => ifExpression(expression, argument),
                        (null, null, null, null, FilterFactory factory) => ifFilterFactory(factory, argument),
                        _ => throw new InvalidOperationException()
                    };

                public void Switch<TArg>(
                    TArg argument,
                    Action<ArgumentsMemberSelector, TArg> ifMemberSelector,
                    Action<Scalar, TArg> ifScalar,
                    Action<Nested, TArg> ifNested,
                    Action<DruidExpression, TArg> ifExpression,
                    Action<FilterFactory, TArg> ifFilterFactory) => Switch(
                        (argument, ifMemberSelector, ifScalar, ifNested, ifExpression, ifFilterFactory),
                        (selector, arg) =>
                        {
                            arg.ifMemberSelector(selector, arg.argument);
                            return 0;
                        },
                        (scalar, arg) =>
                        {
                            arg.ifScalar(scalar, arg.argument);
                            return 0;
                        },
                        (nested, arg) =>
                        {
                            arg.ifNested(nested, arg.argument);
                            return 0;
                        },
                        (expression, arg) =>
                        {
                            arg.ifExpression(expression, arg.argument);
                            return 0;
                        },
                        (filterFactory, arg) =>
                        {
                            arg.ifFilterFactory(filterFactory, arg.argument);
                            return 0;
                        });
            }

            public sealed record ArgumentsMemberSelector(Type SelectedType, string SelectedName, string Name, Type SelectedFromType);
            public sealed record Scalar(Type Type, string Name, object? Value);
            public sealed record Nested(IReadOnlyList<ElementFactoryCall> Calls, string Name, bool Single);
            public sealed record DruidExpression(LambdaExpression? Value, string Name);
            public sealed record FilterFactory(Delegate Value, string Name);
        }
    }

    internal static class SectionFactoryParser
    {
        public static IEnumerable<ElementFactoryCall> Execute(
            LambdaExpression querySectionFactory,
            Type factoryType,
            Type argumentsType,
            Type sectionType)
        {
            InvalidOperationException Invalid(string? details = null, Exception? inner = null) => new(
                $"Invalid expression: {querySectionFactory}." +
                details is null ? "" : "\n" + details,
                inner);
            InvalidOperationException ExpectedToBe(Expression expected, string toBe)
                => Invalid($"Expected {expected} to be {toBe}.");

            Expression EvaluateTernaryCondition(ConditionalExpression ternary)
            {
                try
                {
                    return ternary.EvaluateCondition();
                }
                catch (Exception exception)
                {
                    throw Invalid(exception.Message, exception);
                }
            }

            SelectedProperty GetSelectedProperty(Expression selector)
            {
                InvalidOperationException Unexpected() => ExpectedToBe(selector, $"a property selector from {argumentsType}");
                var lambda = selector as LambdaExpression ?? throw Unexpected();
                if (lambda.Parameters.Count is not 1 || lambda.Parameters[0].Type != argumentsType)
                    throw Unexpected();
                var body = lambda.Body;
                while (body is ConditionalExpression ternary)
                    body = EvaluateTernaryCondition(ternary);
                return SelectedProperty.Get(body);
            }

            ElementFactoryCall Execute(Expression factoryCall, string? resultMemberName)
            {
                InvalidOperationException Unexpected() => ExpectedToBe(factoryCall, $"a method call on {factoryType}");
                static ElementFactoryCall.Parameter.ArgumentsMemberSelector Map(SelectedProperty property, string name)
                    => new(property.Type, property.Name, name, property.SelectedFromType);
                factoryCall = factoryCall.UnwrapUnary();

                if (factoryCall is ConditionalExpression ternary)
                    return Execute(EvaluateTernaryCondition(ternary), resultMemberName);

                var call = factoryCall as MethodCallExpression ?? throw Unexpected();
                var method = call.Method;
                var methodName = method.Name;
                var methodParameters = method.GetParameters();
                var callParameters = call
                    .Arguments
                    .Zip(methodParameters, (arg, @param) =>
                    {
                        var paramType = @param.ParameterType;
                        var paramName = @param.Name!;
                        var openGeneric = paramType.IsGenericType ? paramType.GetGenericTypeDefinition() : null;

                        var isMemberSelector = openGeneric == typeof(QueryElementFactory<>.ColumnSelector<>);
                        if (isMemberSelector)
                            return new ElementFactoryCall.Parameter.Any(Selector: Map(GetSelectedProperty(arg), paramName));

                        var isDruidExpression = openGeneric == typeof(QueryElementFactory<>.DruidExpression);
                        if (isDruidExpression)
                            return new(Expression: new(arg as LambdaExpression, paramName));

                        var isNestedMultiple = paramType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(paramType);
                        var isNestedSingle = !isNestedMultiple && arg is MethodCallExpression call &&
                            call.Method.DeclaringType?.IsGenericType is true &&
                            call.Method.DeclaringType.DeclaringType == typeof(QueryElementFactory<>);
                        if (isNestedMultiple || isNestedSingle)
                            return new(Nested: new(Execute__(arg).ToList(), paramName, isNestedSingle));

                        if (arg is LambdaExpression lambda && lambda.ReturnType == typeof(IFilter))
                            return new(FilterFactory: new(lambda.Compile(), paramName));

                        return new(Scalar: new ElementFactoryCall.Parameter.Scalar(paramType, paramName, arg.GetValue()));
                    })
                    .ToList();
                return new(resultMemberName, methodName, callParameters);
            }


            IEnumerable<ElementFactoryCall> Execute__(Expression sectionFactoryBody)
            {
                sectionFactoryBody = sectionFactoryBody.UnwrapUnary();
                if (sectionFactoryBody.NodeType is ExpressionType.Call)
                {
                    yield return Execute(sectionFactoryBody, null);
                    yield break;
                }

                if (sectionFactoryBody is NewArrayExpression newArray)
                {
                    foreach (var item in newArray.Expressions)
                        yield return Execute(item, null);
                    yield break;
                }

                if (sectionFactoryBody is ConditionalExpression ternary)
                {
                    foreach (var item in Execute__(EvaluateTernaryCondition(ternary)))
                        yield return item;
                    yield break;
                }

                var assignments = sectionFactoryBody.GetPropertyAssignments(Invalid, static (invalid, error) => invalid(error));
                foreach (var (name, assignment) in assignments)
                    yield return Execute(assignment, name);
            }

            return Execute__(querySectionFactory.Body).DistinctBy(call => call.ResultMemberName);
        }
    }
}
