﻿
namespace Apache.Druid.Querying
{
    public class Aggregator : WithType
    {
        public Aggregator(string name, string? type = null) : base(type)
        {
            Name = name;
        }

        public string Name { get; }

        public class WithFieldName : Aggregator
        {
            public WithFieldName(string name, string fieldName, string? type = null) : base(name, type)
            {
                FieldName = fieldName;
            }

            public string FieldName { get; }

            public class WithExpression : WithFieldName
            {
                public WithExpression(string name, string fieldName, string? expression = null, string? type = null) : base(name, fieldName, type)
                {
                    Expression = expression;
                }

                public string? Expression { get; }
            }

            public class WithTimeColumn : WithFieldName
            {
                public WithTimeColumn(string name, string fieldName, string timeColumn, string? type = null) : base(name, fieldName, type)
                {
                    TimeColumn = timeColumn;
                }

                public string TimeColumn { get; }
            }
        }


        internal enum SumMinMaxTypes
        {
            Sum,
            Min,
            Max
        }

        internal enum MeanAnyTypes
        {
            Mean,
            Any
        }

        internal enum FirstLastTypes
        {
            DoubleFirst,
            DoubleLast,
            FloatFirst,
            FloatLast,
            LongFirst,
            LongLast,
            StringFirst,
            StringLast
        }
    }
}
