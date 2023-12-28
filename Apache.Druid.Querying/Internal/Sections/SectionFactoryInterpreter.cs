using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

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

        public Parameter.ArgumentsMemberSelector GetMemberSelectorParameter(string name) => Parameters
            .SingleOrDefault(param => param.Selector?.Name == name)
            ?.Selector
            ?? throw new InvalidOperationException(); // TODO

        public static class Parameter
        {
            public sealed record Any(ArgumentsMemberSelector? Selector = null, Scalar? Scalar = null, Nested? Nested = null)
            {
                public TResult Switch<TResult, TArg>(
                    TArg argument,
                    Func<ArgumentsMemberSelector, TArg, TResult> ifMemberSelector,
                    Func<Scalar, TArg, TResult> ifScalar,
                    Func<Nested, TArg, TResult> ifNested)
                    => (Selector, Scalar, Nested) switch
                    {
                        (ArgumentsMemberSelector selector, null, null) => ifMemberSelector(selector, argument),
                        (null, Scalar scalar, null) => ifScalar(scalar, argument),
                        (null, null, Nested nested) => ifNested(nested, argument),
                        _ => throw new InvalidOperationException()
                    };

                public void Switch<TArg>(
                    TArg argument,
                    Action<ArgumentsMemberSelector, TArg> ifMemberSelector,
                    Action<Scalar, TArg> ifScalar,
                    Action<Nested, TArg> ifNested) => Switch(
                        (argument, ifMemberSelector, ifScalar, ifNested),
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
                        });
            }

            public sealed record ArgumentsMemberSelector(Type MemberType, string MemberName, string Name);
            public sealed record Scalar(Type Type, string Name, object? Value);
            public sealed record Nested(IReadOnlyList<ElementFactoryCall> Calls, string Name);
        }
    }

    internal static class SectionFactoryInterpreter
    {
        public static IEnumerable<ElementFactoryCall> Execute(
            LambdaExpression querySectionFactory,
            Type factoryType,
            Type argumentsType)
        {
            Member GetMember(Expression argumentsMemberSelector)
            {
                var lambda = argumentsMemberSelector as LambdaExpression ?? throw new InvalidOperationException();
                if (lambda.Parameters.Count is not 1 || lambda.Parameters[0].Type != argumentsType)
                    throw new InvalidOperationException();
                return SectionFactoryInterpreter.GetMember(lambda.Body);
            }

            ElementFactoryCall Execute(Expression factoryCall, string? resultMemberName)
            {
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
                        var expectedMemberSelector =
                            paramType.IsGenericType &&
                            paramType.GetGenericTypeDefinition() == typeof(QueryElementFactory<>.ColumnSelector<>);
                        var isNested = paramType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(paramType);
                        ElementFactoryCall.Parameter.Any result = (expectedMemberSelector, isNested) switch
                        {
                            (true, _) => new(Selector: GetMember(arg).Map(paramName)),
                            (_, true) => new(Nested: new(Execute__(arg).ToList(), paramName)),
                            _ => new(Scalar: new ElementFactoryCall.Parameter.Scalar(paramType, paramName, arg.GetValue()))
                        };
                        return result;
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

        private static Member GetMember(Expression selectorBody)
        {
            if (selectorBody is UnaryExpression unary)
                return GetMember(unary.Operand);

            var expression = (MemberExpression)selectorBody;
            var name = expression.Member.Name;
            var property = expression.Member as PropertyInfo ?? throw new InvalidOperationException();
            return new(property.PropertyType, name);
        }

        private readonly record struct Member(Type Type, string Name)
        {
            public ElementFactoryCall.Parameter.ArgumentsMemberSelector Map(string name) => new(Type, Name, name);
        }
    }
}
