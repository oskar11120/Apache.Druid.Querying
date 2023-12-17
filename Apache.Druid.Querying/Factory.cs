using Apache.Druid.Querying.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Apache.Druid.Querying
{
    public static class Factory
    {
        private static string GetColumnName(Expression selectorBody)
        {
            if (selectorBody is UnaryExpression unary)
                return GetColumnName(unary.Operand);

            var expression = (MemberExpression)selectorBody;
            return expression.Member.Name;
        }

        public static class DependentOn
        {
            public abstract class Source<TSource>
            {
                private static IReadOnlyDictionary<string, string>? customColumnNames = null;
                private static string ToCamelCase(string pascalCase) => string.Create(
                    pascalCase.Length,
                    pascalCase,
                    (span, pascalCase) =>
                    {
                        pascalCase.CopyTo(span);
                        span[0] = char.ToLowerInvariant(pascalCase[0]);
                    });

                public delegate TColumn SourceColumnSelector<TColumn>(TSource source);
                protected static string GetColumnName<TColumn>(Expression<SourceColumnSelector<TColumn>> selector)
                {
                    if (customColumnNames is null)
                    {
                        var type = typeof(TSource);
                        type = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(SourceWithVirtualColumns<,>) ?
                            type.GetGenericArguments()[0] :
                            type;
                        var properties = type.GetProperties();
                        customColumnNames = properties.ToDictionary(
                            property => property.Name,
                            property => property.GetCustomAttribute<DataSourceColumn>(true)?.Name ?? property.Name);
                        var hasTimeColumn = customColumnNames.Values.Any(name => name == DataSourceTimeColumn.Name);
                        if (!hasTimeColumn)
                        {
                            var timeProperties = properties
                                .Where(property =>
                                    property.PropertyType == typeof(DateTime) ||
                                    property.PropertyType == typeof(DateTimeOffset))
                                .ToArray();
                            var timeColumnName = timeProperties.Length is 1 ?
                                timeProperties[0].Name :
                                throw new InvalidOperationException(
                                    $"Could not match any property of {type} with column {DataSourceTimeColumn.Name}. " +
                                    $"A property is matched with column {DataSourceTimeColumn.Name} when either:" +
                                    $"{Environment.NewLine}- it's decorated with {nameof(Attribute)} {typeof(DataSourceTimeColumn)}" +
                                    $"{Environment.NewLine}- it's the only property of type {typeof(DateTime)} or {typeof(DateTimeOffset)}.");
                        }
                    }

                    var name = Factory.GetColumnName(selector.Body);
                    name = customColumnNames.GetValueOrDefault(name) ?? name;
                    return ToCamelCase(name);
                }

                public abstract class AndAggregations<TAggregations> : Source<TSource>
                {
                    public delegate TColumn AggregationsColumnSelector<TColumn>(TAggregations aggregations);
                    protected static string GetColumnName<TColumn>(Expression<AggregationsColumnSelector<TColumn>> aggregations)
                        => Factory.GetColumnName(aggregations.Body);
                }
            }

            public abstract class Aggregations<TAggregations>
            {
                public delegate TColumn AggregationsColumnSelector<TColumn>(TAggregations aggregations);
                protected static string GetColumnName<TColumn>(Expression<AggregationsColumnSelector<TColumn>> aggregations)
                    => Factory.GetColumnName(aggregations.Body);

                public abstract class AndPostAggregations<TPostAggregations> : Aggregations<TAggregations>
                {
                    public delegate TColumn PostAggregationsColumnSelector<TColumn>(TPostAggregations postAggregations);
                    protected static string GetColumnName<TColumn>(Expression<PostAggregationsColumnSelector<TColumn>> postAggregations)
                        => Factory.GetColumnName(postAggregations.Body);
                }
            }
        }

        public sealed class VirtualColumns<TColumns>
        {
            public delegate TColumn VirtualColumnSelector<TColumn>(TColumns columns);
            private static string GetColumnName<TColumn>(Expression<VirtualColumnSelector<TColumn>> aggregations)
                => Factory.GetColumnName(aggregations.Body);

            public VirtualColumn.Expression_ Expression<TColumn>(Expression<VirtualColumnSelector<TColumn>> name, string expression)
                => new(GetColumnName(name), expression, DataType.Get<TColumn>());
        }

        public sealed class Filter<TSource> : DependentOn.Source<TSource>
        {
            public Filter And(params Filter[] filters) => new Filter.And(filters);
            public Filter Or(params Filter[] filters) => new Filter.Or(filters);
            public Filter Not(Filter filter) => new Filter.Not(filter);
            public Filter Null<TColumn>(Expression<SourceColumnSelector<TColumn?>> column) => new Filter.Null(GetColumnName(column));
            public Filter In<TColumn>(Expression<SourceColumnSelector<TColumn>> column, IEnumerable<TColumn> values)
                => new Querying.Filter<TColumn>.In(GetColumnName(column), values);
            public Filter Equals<TColumn>(Expression<SourceColumnSelector<TColumn>> column, TColumn matchValue, string? matchValueType = null)
                => new Querying.Filter<TColumn>.Equals(GetColumnName(column), matchValue, matchValueType ?? DataType.Get<TColumn>());
            public Filter Range<TColumn>(
                Expression<SourceColumnSelector<TColumn>> column,
                TColumn? lower = default,
                TColumn? upper = default,
                string? matchValueType = null,
                bool lowerStrict = false,
                bool upperStrict = false)
                => new Querying.Filter<TColumn>.Range(GetColumnName(column), matchValueType ?? DataType.Get<TColumn>(), lower, upper, lowerStrict, upperStrict);
            public Filter Selector<TColumn>(Expression<SourceColumnSelector<TColumn>> dimension, TColumn value)
                => new Querying.Filter<TColumn>.Selector(GetColumnName(dimension), value);
        }

        public sealed class Aggregators<TSource, TAggregations> : DependentOn.Source<TSource>.AndAggregations<TAggregations>
        {
            private static string GetType<TColumn>(SimpleDataType? simpleDataType, string suffix)
                => (simpleDataType ?? DataType.GetSimple<TColumn>()).ToString().ToLowerInvariant() + suffix;

            private static Aggregator Map<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string typeSuffix,
                SimpleDataType? dataType,
                string? expression = null)
                => new Aggregator.WithFieldName.WithExpression(GetColumnName(name), GetColumnName(fieldName), expression, GetType<TAggregationsColumn>(dataType, typeSuffix));

            private static Aggregator Map<TColumn, TTimeColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                string typeSuffix,
                Expression<SourceColumnSelector<TTimeColumn>>? timeColumn,
                SimpleDataType? dataType)
                => new Aggregator.WithFieldName.WithTimeColumn(
                    GetColumnName(name), GetColumnName(fieldName), timeColumn is null ? null : GetColumnName(timeColumn), GetType<TColumn>(dataType, typeSuffix));

            private static Aggregator Map_<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                string typeSuffix,
                SimpleDataType? dataType)
                => Map<TColumn, object?>(name, fieldName, typeSuffix, null, dataType);

            public Aggregator Count<TAggregationsColumn>(Expression<AggregationsColumnSelector<TAggregationsColumn>> name)
                => new(GetColumnName(name), "count");

            public Aggregator Mean<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName)
                => new Aggregator.WithFieldName(GetColumnName(name), GetColumnName(fieldName), "doubleMean");

            public Aggregator Any<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                SimpleDataType? dataType = null)
                => new Aggregator.WithFieldName(GetColumnName(name), GetColumnName(fieldName), GetType<TAggregationsColumn>(dataType, "any"));

            public Aggregator Sum<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                SimpleDataType? dataType = null)
                => Map(name, fieldName, nameof(Sum), dataType);
            public Aggregator Min<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                SimpleDataType? dataType = null)
                => Map(name, fieldName, nameof(Min), dataType);
            public Aggregator Max<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                SimpleDataType? dataType = null)
                => Map(name, fieldName, nameof(Max), dataType);
            public Aggregator Sum<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression,
                SimpleDataType? dataType = null)
                => Map(name, fieldName, nameof(Sum), dataType, expression);
            public Aggregator Min<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression,
                SimpleDataType? dataType = null)
                => Map(name, fieldName, nameof(Min), dataType, expression);
            public Aggregator Max<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression,
                SimpleDataType? dataType = null)
                => Map(name, fieldName, nameof(Max), dataType, expression);

            public Aggregator First<TColumn, TTimeColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                Expression<SourceColumnSelector<TTimeColumn>> timeColumn,
                SimpleDataType? dataType = null)
                => Map(name, fieldName, nameof(First), timeColumn, dataType);
            public Aggregator Last<TColumn, TTimeColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                Expression<SourceColumnSelector<TTimeColumn>> timeColumn,
                SimpleDataType? dataType = null)
                => Map(name, fieldName, nameof(Last), timeColumn, dataType);
            public Aggregator First<TColumn>(
               Expression<AggregationsColumnSelector<TColumn>> name,
               Expression<SourceColumnSelector<TColumn>> fieldName,
                SimpleDataType? dataType = null)
               => Map_(name, fieldName, nameof(First), dataType);
            public Aggregator Last<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                SimpleDataType? dataType = null)
                => Map_(name, fieldName, nameof(Last), dataType);
        }


        public sealed class PostAggregators<TAggregations, TPostAggregations> : DependentOn.Aggregations<TAggregations>.AndPostAggregations<TPostAggregations>
        {
            public PostAggregator Arithmetic<TColumn>(
                Expression<PostAggregationsColumnSelector<TColumn>> name,
                ArithmeticFunction fn,
                IEnumerable<PostAggregator> fields) => new PostAggregator.Arithmetic(GetColumnName(name), fields, fn);

            public PostAggregator Arithmetic<TColumn>(
                Expression<PostAggregationsColumnSelector<TColumn>> name,
                ArithmeticFunction fn,
                params PostAggregator[] fields) => Arithmetic(name, fn, fields.AsEnumerable());

            public PostAggregator FieldAccess<TColumn>(
                Expression<PostAggregationsColumnSelector<TColumn>> name,
                Expression<AggregationsColumnSelector<TColumn>> fieldName,
                bool finalizing = false)
                => new PostAggregator.FieldAccess(GetColumnName(name), GetColumnName(fieldName), finalizing);

            public PostAggregator FieldAccess<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> fieldName,
                bool finalizing = false)
                => new PostAggregator.FieldAccess(GetColumnName(fieldName), GetColumnName(fieldName), finalizing);

            public PostAggregator Constant<TColumn>(
                Expression<PostAggregationsColumnSelector<TColumn>> name,
                TColumn value)
                => new PostAggregator.Constant<TColumn>(GetColumnName(name), value);

            public PostAggregator Expression<TColumn>(
                Expression<PostAggregationsColumnSelector<TColumn>> name,
                string expression)
                => new PostAggregator.Expression_(GetColumnName(name), DataType.Get<TColumn>(), expression);
        }
    }
}
