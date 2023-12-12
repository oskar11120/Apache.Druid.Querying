using Apache.Druid.Querying.Internal;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;

namespace Apache.Druid.Querying
{
    public delegate TColumn FactoryColumnSelector<TSource, TColumn>(TSource source);

    internal class Factory<TSource>
    {
        public delegate TColumn SourceColumnSelector<TColumn>(TSource source);

        private static string GetColumnName(Expression selectorBody)
        {
            var expression = (MemberExpression)selectorBody;
            return expression.Member.Name;
        }

        private static string GetColumnName<TColumn>(Expression<SourceColumnSelector<TColumn>> selector) => GetColumnName(selector.Body);

        public sealed class Filter
        {
            public Querying.Filter.And And(params Querying.Filter[] filters) => new(filters);
            public Querying.Filter.Or Or(params Querying.Filter[] filters) => new(filters);
            public Querying.Filter.Not Not(Querying.Filter filter) => new(filter);
            public Querying.Filter.Null Null<TColumn>(Expression<SourceColumnSelector<TColumn?>> column) => new(GetColumnName(column));
            public Filter<TColumn>.In In<TColumn>(Expression<SourceColumnSelector<TColumn>> column, IEnumerable<TColumn> values)
                => new(GetColumnName(column), values);
            public Filter<TColumn>.Equals Equals<TColumn>(Expression<SourceColumnSelector<TColumn>> column, TColumn matchValue, string? matchValueType = null)
                => new(GetColumnName(column), matchValue, matchValueType ?? DataType.Get<TColumn>());
            public Filter<TColumn>.Range Range<TColumn>(
                Expression<SourceColumnSelector<TColumn>> column,
                TColumn? lower = default,
                TColumn? upper = default,
                string? matchValueType = null,
                bool lowerStrict = false,
                bool upperStrict = false)
                => new(GetColumnName(column), matchValueType ?? DataType.Get<TColumn>(), lower, upper, lowerStrict, upperStrict);
        }

        public sealed class Aggregators<TResult>
        {
            public delegate TColumn ResultColumnSelector<TColumn>(TResult result);

            private static string GetColumnName<TColumn>(Expression<ResultColumnSelector<TColumn>> selector)
                => Factory<TSource>.GetColumnName(selector.Body);

            private static string GetType<TColumn>(string suffix)
                => DataType.GetSimple<TColumn>().ToString() + suffix;

            private static Aggregator.WithFieldName.WithExpression Map<TSourceColumn, TResultColumn>(
                Expression<ResultColumnSelector<TResultColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string typeSuffix,
                string? expression = null)
                => new(GetColumnName(name), Factory<TSource>.GetColumnName(fieldName), expression, GetType<TResultColumn>(typeSuffix));

            private static Aggregator.WithFieldName.WithTimeColumn Map<TColumn, TTimeColumn>(
                Expression<ResultColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                string typeSuffix,
                Expression<SourceColumnSelector<TTimeColumn>>? timeColumn)
                => new(GetColumnName(name), Factory<TSource>.GetColumnName(fieldName), timeColumn is null ? null : Factory<TSource>.GetColumnName(timeColumn), GetType<TColumn>(typeSuffix));

            private static Aggregator.WithFieldName.WithTimeColumn Map_<TColumn>(
                Expression<ResultColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                string typeSuffix)
                => Map<TColumn, object?>(name, fieldName, typeSuffix, null);

            public Aggregator Count<TResultColumn>(Expression<ResultColumnSelector<TResultColumn>> name) => new(GetColumnName(name), "Count");

            public Aggregator.WithFieldName.WithExpression Sum<TColumn>(
                Expression<ResultColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName)
                => Map(name, fieldName, nameof(Sum));
            public Aggregator.WithFieldName.WithExpression Min<TColumn>(
                Expression<ResultColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName)
                => Map(name, fieldName, nameof(Min));
            public Aggregator.WithFieldName.WithExpression Max<TColumn>(
                Expression<ResultColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName)
                => Map(name, fieldName, nameof(Max));
            public Aggregator.WithFieldName.WithExpression Sum<TSourceColumn, TResultColumn>(
                Expression<ResultColumnSelector<TResultColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression)
                => Map(name, fieldName, nameof(Sum));
            public Aggregator.WithFieldName.WithExpression Min<TSourceColumn, TResultColumn>(
                Expression<ResultColumnSelector<TResultColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression)
                => Map(name, fieldName, nameof(Min));
            public Aggregator.WithFieldName.WithExpression Max<TSourceColumn, TResultColumn>(
                Expression<ResultColumnSelector<TResultColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression)
                => Map(name, fieldName, nameof(Max));

            private static Aggregator.WithFieldName.WithTimeColumn First<TColumn, TTimeColumn>(
                Expression<ResultColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                Expression<SourceColumnSelector<TTimeColumn>> timeColumn)
                => Map(name, fieldName, nameof(First), timeColumn);
            private static Aggregator.WithFieldName.WithTimeColumn Last<TColumn, TTimeColumn>(
                Expression<ResultColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                Expression<SourceColumnSelector<TTimeColumn>> timeColumn)
                => Map(name, fieldName, nameof(Last), timeColumn);
            private static Aggregator.WithFieldName.WithTimeColumn First<TColumn>(
               Expression<ResultColumnSelector<TColumn>> name,
               Expression<SourceColumnSelector<TColumn>> fieldName)
               => Map_(name, fieldName, nameof(First));
            private static Aggregator.WithFieldName.WithTimeColumn Last<TColumn>(
                Expression<ResultColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName)
                => Map_(name, fieldName, nameof(Last));
        }
    }
}
