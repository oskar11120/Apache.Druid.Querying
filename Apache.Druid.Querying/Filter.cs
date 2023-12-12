using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

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
    }

    internal static class Filter<TItem, TColumn>
    {
        public delegate TColumn? ColumnSelector(TItem item);

        private static string GetColumnName(Expression<ColumnSelector> selector)
        {
            var expression = (MemberExpression)selector.Body;
            return expression.Member.Name;
        }

        public abstract class WithColumn : Filter
        {
            public WithColumn(Expression<ColumnSelector> column)
            {
                Column = GetColumnName(column);
            }

            public string Column { get; }

            public abstract class AndMatchValueType : WithColumn
            {
                protected AndMatchValueType(Expression<ColumnSelector> column, string? matchValueType = null) : base(column)
                {
                    MatchValueType = matchValueType ?? DataType.Get<TColumn>();
                }

                public string MatchValueType { get; }
            }
        }

        public abstract class WithDimension : Filter
        {
            public WithDimension(Expression<ColumnSelector> column)
            {
                Dimension = GetColumnName(column);
            }

            public string Dimension { get; }
        }

        public new sealed class Equals : WithColumn.AndMatchValueType
        {
            public Equals(Expression<ColumnSelector> column, TColumn matchValue, string? matchValueType = null) : base(column, matchValueType)
            {
                MatchValue = matchValue;
            }

            public TColumn MatchValue { get; }
        }

        public sealed class Null : WithColumn
        {
            public Null(Expression<ColumnSelector> column) : base(column)
            {
            }
        }

        public sealed class In : WithDimension
        {
            public In(Expression<ColumnSelector> column, IEnumerable<TColumn> values) : base(column)
            {
                Values = values;
            }

            public IEnumerable<TColumn> Values { get; }
        }

        public sealed class Range : WithColumn.AndMatchValueType
        {
            public Range(
                Expression<ColumnSelector> column,
                TColumn? lower = default,
                TColumn? upper = default,
                string? matchValueType = null,
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

    internal static class DataType
    {
        private const string String = "String";
        private const char
            L = '<',
            R = '>';
        private static readonly Dictionary<Type, string> simpleMap = new[]
        {
            (new[] { typeof(string), typeof(Guid), typeof(char), typeof(Uri), typeof(Enum) }, String),
            (new[] { typeof(double) }, "Double"),
            (new[] { typeof(float) }, "Float"),
            (new[] { typeof(short), typeof(int), typeof(long), typeof(DateTime), typeof(DateTimeOffset) }, "Long")
        }
        .SelectMany(pair => pair.Item1.Select(type => KeyValuePair.Create(type, pair.Item2)))
        .ToDictionary(pair => pair.Key, pair => pair.Value);

        public static string Get<TValue>()
        {
            var result = new StringBuilder();
            Set(typeof(TValue), result);
            return result.ToString();
        }

        private static void Set(Type type, StringBuilder result)
        {
            if (TrySetSimple(type, result))
                return;
            else if (TrySetNullable(type, result))
                return;
            else if (TrySetArray(type, result))
                return;
            else if (TrySetComplex(type, result))
                return;
            throw new NotSupportedException($"No matching {nameof(DataType)} found for {nameof(type)}.");
        }

        private static bool TrySetSimple(Type type, StringBuilder result)
        {
            simpleMap.TryGetValue(type, out var simple);
            simple ??= type is { IsPrimitive: true, IsEnum: true } ? String : null;
            if (simple is not null)
                result.Append(simple);
            return simple is not null;
        }

        private static bool TrySetNullable(Type type, StringBuilder result)
        {
            var isNullable = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
            if (!isNullable)
                return false;

            var argumentType = type.GetGenericArguments().Single();
            Set(argumentType, result);
            return true;
        }

        private static bool TrySetArray(Type type, StringBuilder result)
        {
            if (!type.IsGenericType || type.GetInterface(nameof(IEnumerable)) is null)
                return false;

            var argumentType = type.GetGenericArguments().Single();
            result.Append("Array").Append(L);
            Set(argumentType, result);
            result.Append(R);
            return true;
        }

        private static bool TrySetComplex(Type type, StringBuilder result)
        {
            var properties = type.GetProperties();
            if (properties.Length == 0)
                return false;

            result.Append("Complex").Append(L).Append("json").Append(R);
            return true;
        }
    }
}
