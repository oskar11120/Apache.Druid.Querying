using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.Elements;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Filter_ = Apache.Druid.Querying.Internal.Elements.Filter;
using Having_ = Apache.Druid.Querying.Internal.Elements.Having;

namespace Apache.Druid.Querying
{
    public static class QueryElementFactory<TArguments>
    {
        public delegate TColumn ColumnSelector<TColumn>(TArguments arguments);
        public delegate string DruidExpression(TArguments arguments);

        public abstract class UsingArgumentColumnNames
        {
            protected readonly PropertyColumnNameMapping.IProvider columnNames;

            protected UsingArgumentColumnNames(PropertyColumnNameMapping.IProvider columnNames)
            {
                this.columnNames = columnNames;
            }

            private string GetColumnName(Expression selectorBody)
            {
                var (_, name, type) = SelectedProperty.Get(selectorBody);
                return columnNames.GetColumnName(type, name);
            }

            protected string GetColumnName<TSelector>(Expression<TSelector> selector)
                => GetColumnName(selector.Body);
        }

        public sealed class Filter : UsingArgumentColumnNames
        {
            public Filter(PropertyColumnNameMapping.IProvider columnNames) : base(columnNames)
            {
            }

            public IFilter True() => Filter_.True.Singleton;
            public IFilter False() => Filter_.False.Singleton;
            public IFilter And(IEnumerable<IFilter> filters) => new Filter_.And(filters);
            public IFilter And(params IFilter[] filters) => And(filters.AsEnumerable());
            public IFilter Or(IEnumerable<IFilter> filters) => new Filter_.Or(filters);
            public IFilter Or(params IFilter[] filters) => Or(filters.AsEnumerable());
            public IFilter Not(IFilter filter) => new Filter_.Not(filter);
            public IFilter Null<TColumn>(Expression<ColumnSelector<TColumn?>> column) => new Filter_.Null(GetColumnName(column));
            public IFilter In<TColumn>(Expression<ColumnSelector<TColumn>> column, IEnumerable<TColumn> values)
                => new Filter<TColumn>.In(GetColumnName(column), values);
            public IFilter Equals<TColumn>(Expression<ColumnSelector<TColumn>> column, TColumn matchValue, string? matchValueType = null)
                => new Filter<TColumn>.Equals(GetColumnName(column), matchValue, matchValueType ?? DataType.Get<TColumn>());
            public IFilter Range<TColumn>(
                Expression<ColumnSelector<TColumn>> column,
                TColumn? lower = default,
                TColumn? upper = default,
                string? matchValueType = null,
                bool lowerOpen = false,
                bool upperOpen = false)
                where TColumn : struct
                => new Filter<TColumn?>.Range(GetColumnName(column), matchValueType ?? DataType.Get<TColumn>(), lower, upper, lowerOpen, upperOpen);
            public IFilter Range<TColumn>(
                Expression<ColumnSelector<TColumn>> column,
                TColumn? lower = default,
                TColumn? upper = default,
                string? matchValueType = null,
                bool lowerOpen = false,
                bool upperOpen = false)
                where TColumn : class
                => new Filter<TColumn>.Range(GetColumnName(column), matchValueType ?? DataType.Get<TColumn>(), lower, upper, lowerOpen, upperOpen);
            public IFilter Selector<TColumn>(Expression<ColumnSelector<TColumn>> dimension, TColumn value)
                => new Filter<TColumn>.Selector(GetColumnName(dimension), value);
            public IFilter Interval<TColumn>(Expression<ColumnSelector<TColumn>> dimension, params Interval[] intervals)
                => new Filter_.Interval(GetColumnName(dimension), intervals);
            public IFilter Expression(Expression<DruidExpression> expression)
                => new Filter_.Expression_(Internal.DruidExpression.Map(expression, columnNames).Expression);
            public IFilter Bound<TColumn>(
                Expression<ColumnSelector<TColumn>> dimension,
                TColumn? lower = default,
                TColumn? upper = default,
                bool lowerStrict = false,
                bool upperStrict = false,
                SortingOrder ordering = SortingOrder.Lexicographic)
                => new Filter<TColumn>.Bound(GetColumnName(dimension), lower, upper, lowerStrict, upperStrict, ordering);
            public IFilter Like<TColumn>(Expression<ColumnSelector<TColumn>> dimension, string pattern, char? escape = null)
                => new Filter_.Like(GetColumnName(dimension), pattern, escape);
            public IFilter Regex<TColumn>(Expression<ColumnSelector<TColumn>> dimension, string pattern)
                => new Filter_.Regex(GetColumnName(dimension), pattern);
        }

        public class MetricSpec : UsingArgumentColumnNames
        {
            public MetricSpec(PropertyColumnNameMapping.IProvider columnNames) : base(columnNames)
            {
            }

            public IMetric Dimension<TColumn>(
                TColumn previousStop,
                SortingOrder ordering = SortingOrder.Lexicographic)
                => new Metric.Dimension<TColumn>(ordering, previousStop);

            public IMetric Dimension(
                SortingOrder ordering = SortingOrder.Lexicographic)
                => new Metric.Dimension<object?>(ordering, null);

            public IMetric Inverted(IMetric metric)
                => new Metric.Inverted(metric);

            public IMetric Numeric<TColumn>(Expression<ColumnSelector<TColumn>> metric)
                => new Metric.Numeric(GetColumnName(metric));
        }

        public class OrderByColumnSpec : UsingArgumentColumnNames
        {
            public OrderByColumnSpec(PropertyColumnNameMapping.IProvider columnNames) : base(columnNames)
            {
            }

            public ILimitSpec.OrderBy OrderBy<TColumn>(
                Expression<ColumnSelector<TColumn>> dimension,
                OrderDirection? direction = null,
                SortingOrder dimensionOrder = SortingOrder.Lexicographic)
                => new LimitSpec.OrderBy(GetColumnName(dimension), dimensionOrder, direction);
        }

        public class Having : UsingArgumentColumnNames
        {
            public Having(PropertyColumnNameMapping.IProvider columnNames) : base(columnNames)
            {
            }

            public IHaving Filter(Func<Filter, IFilter> factory)
            {
                var filter = factory(new Filter(columnNames));
                return new Having_.Filter_(filter);
            }
        }

        public interface INone
        {
            TColumn None<TColumn>();
        }

        public interface IExpression
        {
            TColumn Expression<TColumn>(DruidExpression expression);

            public interface WithOutputType : IExpression
            {
                TColumn Expression<TColumn>(DruidExpression expression, string outputType);
                TColumn Expression<TColumn>(DruidExpression expression, SimpleDataType outputType);
            }
        }

        public interface IVirtualColumns : IExpression.WithOutputType, INone
        {
        }

        public interface IAggregations : INone
        {
            int Count();

            TColumn Mean<TColumn>(ColumnSelector<TColumn> fieldName);

            TColumn Any<TColumn>(ColumnSelector<TColumn> fieldName, SimpleDataType dataType);
            TColumn Any<TColumn>(ColumnSelector<TColumn> fieldName);

            TColumn Sum<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType dataType);
            TColumn Min<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType dataType);
            TColumn Max<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType dataType);
            TColumn Sum<TColumn>(
                ColumnSelector<TColumn> fieldName,
                DruidExpression expression,
                SimpleDataType dataType);
            TColumn Min<TColumn>(
                ColumnSelector<TColumn> fieldName,
                DruidExpression expression,
                SimpleDataType dataType);
            TColumn Max<TColumn>(
                ColumnSelector<TColumn> fieldName,
                DruidExpression expression,
                SimpleDataType dataType);

            TColumn Sum<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Min<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Max<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Sum<TColumn>(
                ColumnSelector<TColumn> fieldName,
                DruidExpression expression);
            TColumn Min<TColumn>(
                ColumnSelector<TColumn> fieldName,
                DruidExpression expression);
            TColumn Max<TColumn>(
                ColumnSelector<TColumn> fieldName,
                DruidExpression expression);

            TColumn First<TColumn, TTimeColumn>(
                ColumnSelector<TColumn> fieldName,
                ColumnSelector<TTimeColumn> timeColumn,
                SimpleDataType dataType);
            TColumn Last<TColumn, TTimeColumn>(
                ColumnSelector<TColumn> fieldName,
                ColumnSelector<TTimeColumn> timeColumn,
                SimpleDataType dataType);
            TColumn First<TColumn>(
               ColumnSelector<TColumn> fieldName,
                SimpleDataType dataType);
            TColumn Last<TColumn>(
                ColumnSelector<TColumn> fieldName,
                SimpleDataType dataType);

            TColumn First<TColumn, TTimeColumn>(
                ColumnSelector<TColumn> fieldName,
                ColumnSelector<TTimeColumn> timeColumn);
            TColumn Last<TColumn, TTimeColumn>(
                ColumnSelector<TColumn> fieldName,
                ColumnSelector<TTimeColumn> timeColumn);
            TColumn First<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Last<TColumn>(ColumnSelector<TColumn> fieldName);

            string First<TTimeColumn>(
                ColumnSelector<string> fieldName,
                ColumnSelector<TTimeColumn> timeColumn,
                long maxStringBytes,
                SimpleDataType dataType);
            string Last<TTimeColumn>(
                ColumnSelector<string> fieldName,
                ColumnSelector<TTimeColumn> timeColumn,
                long maxStringBytes,
                SimpleDataType dataType);
            string First(
                ColumnSelector<string> fieldName,
                long maxStringBytes,
                SimpleDataType dataType);
            string Last(
                ColumnSelector<string> fieldName,
                long maxStringBytes,
                SimpleDataType dataType);

            string First<TTimeColumn>(
                ColumnSelector<string> fieldName,
                ColumnSelector<TTimeColumn> timeColumn,
                long maxStringBytes);
            string Last<TTimeColumn>(
                ColumnSelector<string> fieldName,
                ColumnSelector<TTimeColumn> timeColumn,
                long maxStringBytes);
            string First(
                ColumnSelector<string> fieldName,
                long maxStringBytes);
            string Last(
                ColumnSelector<string> fieldName,
                long maxStringBytes);

            TColumn Expression<TColumn, TInitialValue>(
                TInitialValue initialValue,
                DruidExpression fold);
            TColumn Expression<TColumn, TInitialValue>(
                TInitialValue initialValue,
                DruidExpression fold,
                DruidExpression combine);
            TColumn Expression<TColumn, TInitialValue>(
                TInitialValue initialValue,
                DruidExpression fold,
                DruidExpression combine,
                DruidExpression compare);
            TColumn Expression<TColumn, TInitialValue>(
                TInitialValue initialValue,
                DruidExpression fold,
                DruidExpression combine,
                DruidExpression compare,
                DruidExpression finalize);
            TColumn Expression<TColumn, TInitialValue>(
                TInitialValue initialValue,
                string? accumulatorIdentifier,
                DruidExpression fold,
                DruidExpression? combine,
                DruidExpression? compare,
                DruidExpression? finalize,
                DruidExpression? initialValueCombine,
                bool? isNullUnlessAggregated,
                bool? shouldAggregateNullInputs,
                bool? shouldCombineAggregateNullInputs,
                long? maxSizeBytes);
            TColumn Expression<TColumn>(
                TColumn initialValue,
                DruidExpression fold);
            TColumn Expression<TColumn>(
                TColumn initialValue,
                DruidExpression fold,
                DruidExpression combine);
            TColumn Expression<TColumn>(
                TColumn initialValue,
                DruidExpression fold,
                DruidExpression combine,
                DruidExpression compare);
            TColumn Expression<TColumn>(
                TColumn initialValue,
                DruidExpression fold,
                DruidExpression combine,
                DruidExpression compare,
                DruidExpression finalize);
            TColumn Expression<TColumn>(
                TColumn initialValue,
                string? accumulatorIdentifier,
                DruidExpression fold,
                DruidExpression? combine,
                DruidExpression? compare,
                DruidExpression? finalize,
                DruidExpression? initialValueCombine,
                bool? isNullUnlessAggregated,
                bool? shouldAggregateNullInputs,
                bool? shouldCombineAggregateNullInputs,
                long? maxSizeBytes);

            TColumn Filtered<TColumn>(
                Func<QueryElementFactory<TArguments>.Filter, IFilter> filter,
                TColumn aggregator);
        }

        public interface IPostAggregators : IExpression.WithOutputType, INone
        {
            TColumn Arithmetic<TColumn>(ArithmeticFunction fn, IEnumerable<TColumn> fields);
            TColumn Arithmetic<TColumn>(ArithmeticFunction fn, params TColumn[] fields);
            TColumn FieldAccess<TColumn>(ColumnSelector<TColumn> fieldName, bool finalizing);
            TColumn FieldAccess<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Constant<TColumn>(TColumn value);
            TColumn Expression<TColumn>(DruidExpression expression, string outputType, string? ordering);
            TColumn Expression<TColumn>(DruidExpression expression, SimpleDataType outputType, string? ordering);
        }

        public interface IDimensions : INone
        {
            TColumn Default<TColumn>(ColumnSelector<TColumn> dimension);
            TColumn Default<TColumn>(ColumnSelector<TColumn> dimension, SimpleDataType outputType);
        }
    }
}
