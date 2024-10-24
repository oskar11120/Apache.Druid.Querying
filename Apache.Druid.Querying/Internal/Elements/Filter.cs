﻿using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying.Internal.Elements
{
    internal abstract class Filter : WithType, IFilter
    {
        public sealed class True : Filter
        {
            public static readonly True Singleton = new();
        }

        public sealed class False : Filter
        {
            public static readonly True Singleton = new();
        }

        public sealed class And : Filter
        {
            public IEnumerable<IFilter> Fields { get; }

            public And(IEnumerable<IFilter> fields)
            {
                Fields = fields;
            }
        }

        public sealed class Or : Filter
        {
            public IEnumerable<IFilter> Fields { get; }

            public Or(IEnumerable<IFilter> fields)
            {
                Fields = fields;
            }
        }

        public sealed class Not : Filter
        {
            public IFilter Field { get; }

            public Not(IFilter field)
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

        public abstract class WithDimension : Filter
        {
            public WithDimension(string dimension)
            {
                Dimension = dimension;
            }

            public string Dimension { get; }
        }

        public sealed class Interval : Filter
        {
            public Interval(string dimension, IEnumerable<Querying.Interval> intervals)
            {
                Dimension = dimension;
                Intervals = intervals;
            }

            public string Dimension { get; }
            public IEnumerable<Querying.Interval> Intervals { get; }
        }

        public sealed class Expression_ : Filter
        {
            public string Expression { get; }

            public Expression_(string expression)
            {
                Expression = expression;
            }
        }

        public sealed class Like : WithDimension
        {
            public Like(string dimension, string pattern, char? escape) : base(dimension)
            {
                Pattern = pattern;
                Escape = escape;
            }

            public string Pattern { get; }
            public char? Escape { get; }
        }

        public sealed class Regex : WithDimension
        {
            public Regex(string dimension, string pattern) : base(dimension)
            {
                Pattern = pattern;
            }

            public string Pattern { get; }
        }
    }

    internal abstract class Filter<TColumn> : Filter.WithColumn
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

        public new abstract class WithDimension : Filter
        {
            public WithDimension(string dimension)
            {
                Dimension = dimension;
            }

            public string Dimension { get; }
        }

        public new sealed class Equals : WithMatchValueType
        {
            public Equals(string column, JsonNode matchValue, string matchValueType) : base(column, matchValueType)
            {
                MatchValue = matchValue;
            }

            public JsonNode MatchValue { get; }
        }

        public sealed class In : WithDimension
        {
            public In(string column, JsonNode values) : base(column)
            {
                Values = values;
            }

            public JsonNode Values { get; }
        }

        public sealed class Range : WithMatchValueType
        {
            public Range(
                string column,
                string matchValueType,
                JsonNode? lower = default,
                JsonNode? upper = default,
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

            public JsonNode? Lower { get; }
            public JsonNode? Upper { get; }
            public bool LowerOpen { get; }
            public bool UpperOpen { get; }
        }

        public sealed class Selector : WithDimension
        {
            public Selector(string dimension, JsonNode value) : base(dimension)
            {
                Value = value;
            }

            public JsonNode Value { get; }
        }

        public sealed class Bound : WithDimension
        {
            public Bound(
                string dimension,
                JsonNode? lower,
                JsonNode? upper,
                bool lowerStrict,
                bool upperStrict,
                SortingOrder ordering)
                : base(dimension)
            {
                Lower = lower;
                Upper = upper;
                LowerStrict = lowerStrict;
                UpperStrict = upperStrict;
                Ordering = ordering;
            }

            public JsonNode? Lower { get; }
            public JsonNode? Upper { get; }
            public bool LowerStrict { get; }
            public bool UpperStrict { get; }
            public SortingOrder Ordering { get; }
        }
    }
}