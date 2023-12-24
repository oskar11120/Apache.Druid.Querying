using System.Collections.Generic;

namespace Apache.Druid.Querying.Elements
{
    public class GroupByLimit : WithType
    {
        public GroupByLimit(int? limit, int? offset, IEnumerable<OrderBy>? columns) : base("default")
        {
            Limit = limit;
            Offset = offset;
            Columns = columns;
        }

        public int? Limit { get; }
        public int? Offset { get; }
        public IEnumerable<OrderBy>? Columns { get; }

        public class OrderBy
        {
            public OrderBy(string dimension, OrderDirection direction, SortingOrder dimensionOrder)
            {
                Dimension = dimension;
                Direction = direction;
                DimensionOrder = dimensionOrder;
            }

            public string Dimension { get; }
            public OrderDirection Direction { get; }
            public SortingOrder DimensionOrder { get; }
        }
    }
}
