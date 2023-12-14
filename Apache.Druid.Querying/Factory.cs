using Apache.Druid.Querying.Internal;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Apache.Druid.Querying
{
    public static class Factory
    {
        private static string GetColumnName(Expression selectorBody)
        {
            var expression = (MemberExpression)selectorBody;
            return expression.Member.Name;
        }

        public static class DependentOn
        {
            public abstract class Source<TSource>
            {
                public delegate TColumn SourceColumnSelector<TColumn>(TSource source);
                protected static string GetColumnName<TColumn>(Expression<SourceColumnSelector<TColumn>> selector)
                    => Factory.GetColumnName(selector.Body);

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
        }

        public sealed class Aggregators<TSource, TAggregations> : DependentOn.Source<TSource>.AndAggregations<TAggregations>
        {
            private static string GetSimpleDataType<TColumn>(string suffix)
                => DataType.GetSimple<TColumn>().ToString() + suffix;

            private static Aggregator.WithFieldName.WithExpression Map<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
            string typeSuffix,
            string? expression = null)
                => new(GetColumnName(name), GetColumnName(fieldName), expression, GetSimpleDataType<TAggregationsColumn>(typeSuffix));

            private static Aggregator.WithFieldName.WithTimeColumn Map<TColumn, TTimeColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
            string typeSuffix,
            Expression<SourceColumnSelector<TTimeColumn>>? timeColumn)
            => new(GetColumnName(name), GetColumnName(fieldName), timeColumn is null ? null : GetColumnName(timeColumn), GetSimpleDataType<TColumn>(typeSuffix));

            private static Aggregator.WithFieldName.WithTimeColumn Map_<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                string typeSuffix)
                => Map<TColumn, object?>(name, fieldName, typeSuffix, null);

            public Aggregator Count<TAggregationsColumn>(Expression<AggregationsColumnSelector<TAggregationsColumn>> name) => new(GetColumnName(name), "Count");

            public Aggregator.WithFieldName Mean<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName)
                => new(GetColumnName(name), GetColumnName(fieldName), "DoubleMean");

            public Aggregator.WithFieldName Any<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
            Expression<SourceColumnSelector<TSourceColumn>> fieldName)
                => new(GetColumnName(name), GetColumnName(fieldName), GetSimpleDataType<TAggregationsColumn>("Any"));

            public Aggregator.WithFieldName.WithExpression Sum<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName)
                => Map(name, fieldName, nameof(Sum));
            public Aggregator.WithFieldName.WithExpression Min<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName)
                => Map(name, fieldName, nameof(Min));
            public Aggregator.WithFieldName.WithExpression Max<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName)
                => Map(name, fieldName, nameof(Max));
            public Aggregator.WithFieldName.WithExpression Sum<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression)
                => Map(name, fieldName, nameof(Sum));
            public Aggregator.WithFieldName.WithExpression Min<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression)
                => Map(name, fieldName, nameof(Min));
            public Aggregator.WithFieldName.WithExpression Max<TSourceColumn, TAggregationsColumn>(
                Expression<AggregationsColumnSelector<TAggregationsColumn>> name,
                Expression<SourceColumnSelector<TSourceColumn>> fieldName,
                string expression)
                => Map(name, fieldName, nameof(Max));

            public Aggregator.WithFieldName.WithTimeColumn First<TColumn, TTimeColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                Expression<SourceColumnSelector<TTimeColumn>> timeColumn)
                => Map(name, fieldName, nameof(First), timeColumn);
            public Aggregator.WithFieldName.WithTimeColumn Last<TColumn, TTimeColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName,
                Expression<SourceColumnSelector<TTimeColumn>> timeColumn)
                => Map(name, fieldName, nameof(Last), timeColumn);
            public Aggregator.WithFieldName.WithTimeColumn First<TColumn>(
               Expression<AggregationsColumnSelector<TColumn>> name,
               Expression<SourceColumnSelector<TColumn>> fieldName)
               => Map_(name, fieldName, nameof(First));
            public Aggregator.WithFieldName.WithTimeColumn Last<TColumn>(
                Expression<AggregationsColumnSelector<TColumn>> name,
                Expression<SourceColumnSelector<TColumn>> fieldName)
                => Map_(name, fieldName, nameof(Last));
        }


        public sealed class PostAggregators<TAggregations, TPostAggregations> : DependentOn.Aggregations<TAggregations>.AndPostAggregations<TPostAggregations>
        {
        }
    }
}
