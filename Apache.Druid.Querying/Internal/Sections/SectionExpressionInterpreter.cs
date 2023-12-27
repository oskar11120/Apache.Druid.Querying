using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Apache.Druid.Querying.Internal.QuerySectionFactory
{
    internal sealed record FactoryCall(
        string ResultMemberName,
        string MethodName,
        IEnumerable<FactoryCall.Parameter.Any> Parameters)
    {
        public static class Parameter
        {
            public sealed record Any(ArgumentsMemberSelector? Selector, Scalar? Scalar)
            {
                public TResult Switch<TResult, TArg>(
                    TArg argument,
                    Func<ArgumentsMemberSelector, TArg, TResult> ifMemberSelector,
                    Func<Scalar, TArg, TResult> ifScalar)
                    => (Selector, Scalar) switch
                    {
                        (ArgumentsMemberSelector selector, null) => ifMemberSelector(selector, argument),
                        (null, Scalar scalar) => ifScalar(scalar, argument),
                        _ => throw new InvalidOperationException()
                    };

                public void Switch<TArg>(
                    TArg argument,
                    Action<ArgumentsMemberSelector, TArg> ifMemberSelector,
                    Action<Scalar, TArg> ifScalar) => Switch(
                        (argument, ifMemberSelector, ifScalar),
                        (selector, arg) =>
                        {
                            arg.ifMemberSelector(selector, arg.argument);
                            return 0;
                        },
                        (scalar, arg) =>
                        {
                            arg.ifScalar(scalar, arg.argument);
                            return 0;
                        });
            }

            public sealed record ArgumentsMemberSelector(Type Type, string Name);
            public sealed record Scalar(Type Type, string Name, object? Value);
        }
    }

    internal static class SectionExpressionInterpreter
    {
        public static IEnumerable<FactoryCall> Execute(
            LambdaExpression factoryExpression,
            Type factoryType,
            Type argumentsType)
        {
            FactoryCall.Parameter.ArgumentsMemberSelector Execute_(Expression argumentsMemberSelector)
            {
                var lambda = argumentsMemberSelector as LambdaExpression ?? throw new InvalidOperationException();
                if (lambda.Parameters.Count is not 1 || lambda.Parameters[0].Type != argumentsType)
                    throw new InvalidOperationException();
                return ExecuteOnBody(lambda.Body);
            }

            FactoryCall Execute(Expression factoryCall, string resultMemberName)
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
                        var expectedArgumentsMemberSelector =
                            paramType.IsGenericType &&
                            paramType.GetGenericTypeDefinition() == typeof(QueryElementFactory<>.ColumnSelector<>);
                        FactoryCall.Parameter.Any result = expectedArgumentsMemberSelector ?
                            new(Execute_(arg), null) :
                            new(null, new FactoryCall.Parameter.Scalar(paramType, @param.Name!, arg.GetValue()));
                        return result;
                    });
                return new(resultMemberName, methodName, callParameters);
            }


            IEnumerable<FactoryCall> Execute__()
            {
                var body = factoryExpression.Body;
                var init = body as MemberInitExpression;
                var @new = init is null ?
                    body as NewExpression ?? throw new InvalidOperationException() :
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

            return Execute__().DistinctBy(call => call.ResultMemberName);
        }

        private static FactoryCall.Parameter.ArgumentsMemberSelector ExecuteOnBody(Expression selectorBody)
        {
            if (selectorBody is UnaryExpression unary)
                return ExecuteOnBody(unary.Operand);

            var expression = (MemberExpression)selectorBody;
            var name = expression.Member.Name;
            var property = expression.Member as PropertyInfo ?? throw new InvalidOperationException();
            return new(property.PropertyType, name);
        }
    }
}
