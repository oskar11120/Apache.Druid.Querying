using Apache.Druid.Querying.Internal;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Apache.Druid.Querying
{
    public sealed class TimeSeriesQueryBuilder<TSource>
    {
        public string QueryType { get; } = "timeseries";
    }

    public sealed class FilterFactory<TSource>
    {
        public delegate TColumn ColumnSelector<TColumn>(TSource source);

        private static string GetColumnName<TColumn>(Expression<ColumnSelector<TColumn>> selector)
        {
            var expression = (MemberExpression)selector.Body;
            return expression.Member.Name;
        }

        public Filter.And And(params Filter[] filters) => new(filters);
        public Filter.Or Or(params Filter[] filters) => new(filters);
        public Filter.Not Not(Filter filter) => new(filter);
        public Filter.Null Null<TColumn>(Expression<ColumnSelector<TColumn?>> column) => new(GetColumnName(column));
        public Filter<TColumn>.In In<TColumn>(Expression<ColumnSelector<TColumn>> column, IEnumerable<TColumn> values)
            => new(GetColumnName(column), values);
        public Filter<TColumn>.Equals Equals<TColumn>(Expression<ColumnSelector<TColumn>> column, TColumn matchValue, string? matchValueType = null)
            => new(GetColumnName(column), matchValue, matchValueType ?? DataType.Get<TColumn>());
        public Filter<TColumn>.Range Range<TColumn>(
            Expression<ColumnSelector<TColumn>> column,
            TColumn? lower = default,
            TColumn? upper = default,
            string? matchValueType = null,
            bool lowerStrict = false,
            bool upperStrict = false)
            => new(GetColumnName(column), matchValueType ?? DataType.Get<TColumn>(), lower, upper, lowerStrict, upperStrict);
    }
}
