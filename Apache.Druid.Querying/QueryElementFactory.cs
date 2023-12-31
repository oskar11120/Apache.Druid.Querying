using Apache.Druid.Querying.Internal;
using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Filter_ = Apache.Druid.Querying.Internal.QuerySectionFactory.Filter;
using Having_ = Apache.Druid.Querying.Internal.QuerySectionFactory.Having;

namespace Apache.Druid.Querying
{
    public static class QueryElementFactory<TArguments>
    {
        public delegate TColumn ColumnSelector<TColumn>(TArguments arguments);

        public abstract class UsingArgumentColumnNames
        {
            protected readonly IArgumentColumnNameProvider columnNames;

            protected UsingArgumentColumnNames(IArgumentColumnNameProvider columnNames)
            {
                this.columnNames = columnNames;
            }

            private string GetColumnName(Expression selectorBody)
            {
                if (selectorBody is UnaryExpression unary)
                    return GetColumnName(unary.Operand);

                var expression = (MemberExpression)selectorBody;
                var memberName = expression.Member.Name;
                return columnNames.Get(memberName);
            }

            protected string GetColumnName<TSelector>(Expression<TSelector> selector)
                => GetColumnName(selector.Body);
        }

        public sealed class Filter : UsingArgumentColumnNames
        {
            public Filter(IArgumentColumnNameProvider columnNames) : base(columnNames)
            {
            }

            public IFilter And(params IFilter[] filters) => new Filter_.And(filters);
            public IFilter Or(params IFilter[] filters) => new Filter_.Or(filters);
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
        }

        public class MetricSpec : UsingArgumentColumnNames
        {
            public MetricSpec(IArgumentColumnNameProvider columnNames) : base(columnNames)
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
            public OrderByColumnSpec(IArgumentColumnNameProvider columnNames) : base(columnNames)
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
            public Having(IArgumentColumnNameProvider columnNames) : base(columnNames)
            {
            }

            public IHaving Filter(Func<Filter, IFilter> factory)
            {
                var filter = factory(new Filter(columnNames));
                return new Having_.Filter_(filter);
            }
        }
        public interface IVirtualColumns
        {
            TColumn Expression<TColumn>(string expression);
        }

        public interface IAggregations
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
                string expression,
                SimpleDataType dataType);
            TColumn Min<TColumn>(
                ColumnSelector<TColumn> fieldName,
                string expression,
                SimpleDataType dataType);
            TColumn Max<TColumn>(
                ColumnSelector<TColumn> fieldName,
                string expression,
                SimpleDataType dataType);

            TColumn Sum<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Min<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Max<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Sum<TColumn>(
                ColumnSelector<TColumn> fieldName,
                string expression);
            TColumn Min<TColumn>(
                ColumnSelector<TColumn> fieldName,
                string expression);
            TColumn Max<TColumn>(
                ColumnSelector<TColumn> fieldName,
                string expression);

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
        }


        public interface IPostAggregators
        {
            TColumn Arithmetic<TColumn>(ArithmeticFunction fn, IEnumerable<TColumn> fields);
            TColumn Arithmetic<TColumn>(ArithmeticFunction fn, params TColumn[] fields);
            TColumn FieldAccess<TColumn>(ColumnSelector<TColumn> fieldName, bool finalizing);
            TColumn FieldAccess<TColumn>(ColumnSelector<TColumn> fieldName);
            TColumn Constant<TColumn>(TColumn value);
            TColumn Expression<TColumn>(string expression);
        }

        public interface IDimensions
        {
            TColumn Default<TColumn>(ColumnSelector<TColumn> dimension);
            TColumn Default<TColumn>(ColumnSelector<TColumn> dimension, SimpleDataType outputType);
        }
    }
}
