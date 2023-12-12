using System;
using System.Collections.Generic;

namespace Apache.Druid.Querying
{
    public abstract class Filter : WithType
    {
        public sealed class And : Filter
        {
            public IEnumerable<Filter> Fields { get; }

            public And(IEnumerable<Filter> fields)
            {
                Fields = fields;
            }
        }

        public sealed class Or : Filter
        {
            public IEnumerable<Filter> Fields { get; }

            public Or(IEnumerable<Filter> fields)
            {
                Fields = fields;
            }
        }

        public sealed class Not : Filter
        {
            public Filter Field { get; }

            public Not(Filter field)
            {
                Field = field;
            }
        }

        public sealed class Null : WithColumn
        {
            public Null(string column) : base(column)
            {
            }
        }

        public abstract class WithColumn : Filter
        {
            public WithColumn(string column)
            {
                Column = column;
            }

            public string Column { get; }
        }
    }

    public abstract class Filter<TColumn> : Filter.WithColumn
    {
        protected Filter(string column) : base(column)
        {
        }

        public abstract class WithMatchValueType : Filter<TColumn>
        {
            protected WithMatchValueType(string column, string matchValueType) : base(column)
            {
                MatchValueType = matchValueType;
            }

            public string MatchValueType { get; }
        }

        public abstract class WithDimension : Filter
        {
            public WithDimension(string dimension)
            {
                Dimension = dimension;
            }

            public string Dimension { get; }
        }

        public new sealed class Equals : WithMatchValueType
        {
            public Equals(string column, TColumn matchValue, string matchValueType) : base(column, matchValueType)
            {
                MatchValue = matchValue;
            }

            public TColumn MatchValue { get; }
        }

        public sealed class In : WithDimension
        {
            public In(string column, IEnumerable<TColumn> values) : base(column)
            {
                Values = values;
            }

            public IEnumerable<TColumn> Values { get; }
        }

        public sealed class Range : WithMatchValueType
        {
            public Range(
                string column,
                string matchValueType,
                TColumn? lower = default,
                TColumn? upper = default,
                bool lowerStrict = false,
                bool upperStrict = false)
                : base(column, matchValueType)
            {
                Lower = lower;
                Upper = upper;
                LowerOpen = lowerStrict;
                UpperOpen = upperStrict;

                if ((lower ?? upper) is null)
                {
                    throw new ArgumentException("At least one bound has to be specified.")
                    {
                        Data = { [nameof(Filter)] = this }
                    };
                }
            }

            public TColumn? Lower { get; }
            public TColumn? Upper { get; }
            public bool LowerOpen { get; }
            public bool UpperOpen { get; }
        }
    }
}
}
