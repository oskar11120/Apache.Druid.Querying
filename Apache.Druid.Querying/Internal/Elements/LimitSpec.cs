using System.Collections.Generic;

namespace Apache.Druid.Querying.Internal.QuerySectionFactory
{
    internal sealed class LimitSpec : WithType, ILimitSpec
    {
        public LimitSpec(int? limit, int? offset, IEnumerable<ILimitSpec.OrderBy>? columns) : base("default")
        {
            Limit = limit;
            Offset = offset;
            Columns = columns;
        }

        public int? Limit { get; }
        public int? Offset { get; }
        public IEnumerable<ILimitSpec.OrderBy>? Columns { get; }

        public sealed class OrderBy : ILimitSpec.OrderBy
        {
            public OrderBy(string dimension, SortingOrder dimensionOrder, OrderDirection? direction = null)
            {
                Dimension = dimension;
                Direction = direction;
                DimensionOrder = dimensionOrder;
            }

            public string Dimension { get; }
            public OrderDirection? Direction { get; }
            public SortingOrder DimensionOrder { get; }
        }
    }
}
