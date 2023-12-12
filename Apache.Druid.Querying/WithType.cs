
using System.Linq.Expressions;

namespace Apache.Druid.Querying
{
    public abstract class WithType
    {
        public string Type { get; }

        public WithType(string? type = null)
        {
            Type = type ?? GetType().Name;
        }

        public abstract class WithColumnSelector<TItem, TColumn> : WithType
        {
            public delegate TColumn? ColumnSelector(TItem item);

            public WithColumnSelector(string? type = null) : base(type)
            {

            }

            protected static string GetColumnName(Expression<ColumnSelector> selector)
            {
                var expression = (MemberExpression)selector.Body;
                return expression.Member.Name;
            }
        }
    }
}
