using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Internal.QuerySectionFactory
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
            => TryGetMemberSelectorParameter(name) ?? throw new InvalidOperationException(); // TODO

        public static class Parameter
        {
            public sealed record Any(ArgumentsMemberSelector? Selector = null, Scalar? Scalar = null, Nested? Nested = null, DruidExpression? Expression = null)
            {
                public TResult Switch<TResult, TArg>(
                    TArg argument,
                    Func<ArgumentsMemberSelector, TArg, TResult> ifMemberSelector,
                    Func<Scalar, TArg, TResult> ifScalar,
                    Func<Nested, TArg, TResult> ifNested,
                    Func<DruidExpression, TArg, TResult> ifExpression)
                    => (Selector, Scalar, Nested, Expression) switch
                    {
                        (ArgumentsMemberSelector selector, null, null, null) => ifMemberSelector(selector, argument),
                        (null, Scalar scalar, null, null) => ifScalar(scalar, argument),
                        (null, null, Nested nested, null) => ifNested(nested, argument),
                        (null, null, null, DruidExpression expression) => ifExpression(expression, argument),
                        _ => throw new InvalidOperationException()
                    };

                public void Switch<TArg>(
                    TArg argument,
                    Action<ArgumentsMemberSelector, TArg> ifMemberSelector,
                    Action<Scalar, TArg> ifScalar,
                    Action<Nested, TArg> ifNested,
                    Action<DruidExpression, TArg> ifExpression) => Switch(
                        (argument, ifMemberSelector, ifScalar, ifNested, ifExpression),
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
                        });
            }

            public sealed record ArgumentsMemberSelector(Type SelectedType, string SelectedName, string Name, Type SelectedFromType);
            public sealed record Scalar(Type Type, string Name, object? Value);
            public sealed record Nested(IReadOnlyList<ElementFactoryCall> Calls, string Name);
            public sealed record DruidExpression(LambdaExpression? Value, string Name);
        }
    }

    internal static class SectionFactoryParser
    {
        public static IEnumerable<ElementFactoryCall> Execute(
            LambdaExpression querySectionFactory,
            Type factoryType,
            Type argumentsType)
        {
            SelectedProperty GetSelectedProperty(Expression selector)
            {
                var lambda = selector as LambdaExpression ?? throw new InvalidOperationException();
                if (lambda.Parameters.Count is not 1 || lambda.Parameters[0].Type != argumentsType)
                    throw new InvalidOperationException();
                return SelectedProperty.Get(lambda.Body);
            }

            ElementFactoryCall Execute(Expression factoryCall, string? resultMemberName)
            {
                static ElementFactoryCall.Parameter.ArgumentsMemberSelector Map(SelectedProperty property, string name)
                    => new(property.Type, property.Name, name, property.SelectedFromType);
                factoryCall = factoryCall.UnwrapUnary();
                var call = factoryCall as MethodCallExpression ?? throw new InvalidOperationException();
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

                        var isNested = paramType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(paramType);
                        if (isNested)
                            return new(Nested: new(Execute__(arg).ToList(), paramName));

                        return new(Scalar: new ElementFactoryCall.Parameter.Scalar(paramType, paramName, arg.GetValue()));
                    })
                    .ToList();
                return new(resultMemberName, methodName, callParameters);
            }


            IEnumerable<ElementFactoryCall> Execute__(Expression sectionFactoryBody)
            {
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

                var init = sectionFactoryBody as MemberInitExpression;
                var @new = init is null ?
                    sectionFactoryBody as NewExpression ?? throw new InvalidOperationException() :
                    init.NewExpression;

                if (init is not null)
                {
                    foreach (var binding in init.Bindings)
                    {
                        var assigment = binding as MemberAssignment ?? throw new InvalidOperationException();
                        var name = assigment.Member.Name;
                        yield return Execute(assigment.Expression, name);
                    }
                }

                var propertyNames = @new
                    .Type
                    .GetProperties()
                    .Select(property => property.Name)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                foreach (var (argument, parameter) in @new.Arguments.Zip(@new.Constructor!.GetParameters()))
                {
                    if (!propertyNames.TryGetValue(parameter.Name!, out var name))
                        throw new InvalidOperationException();
                    yield return Execute(argument, name);
                }
            }

            return Execute__(querySectionFactory.Body).DistinctBy(call => call.ResultMemberName);
        }


    }
}
