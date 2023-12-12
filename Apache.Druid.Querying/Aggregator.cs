namespace Apache.Druid.Querying
{
    internal abstract class Aggregator : WithType
    {
        public Aggregator(string name, string fieldName, string? type = null) : base(type)
        {
            Name = name;
            FieldName = fieldName;
        }

        public string Name { get; }
        public string FieldName { get; }

        public abstract class WithEnumType<TType> : Aggregator where TType : System.Enum
        {
            protected WithEnumType(string name, string fieldName, TType type) : base(name, fieldName, type.ToString())
            {
            }
        }

        public enum SumMinMaxTypes
        {
            LongSum,
            DoubleSum,
            FloatSum,
            DoubleMin,
            DoubleMax,
            FloatMin,
            FloatMax,
            LongMin,
            LongMax
        }

        public sealed class SumMinMax : WithEnumType<SumMinMaxTypes>
        {
            public SumMinMax(string name, string fieldName, SumMinMaxTypes type) : base(name, fieldName, type)
            {
            }
        }

        public enum CountMinAnyTypes
        {
            Count,
            DoubleMean,
            DoubleAny,
            LongAny,
            CountAny,
            StringAny
        }

        public sealed class CountMinAny : WithEnumType<CountMinAnyTypes>
        {
            public CountMinAny(string name, string fieldName, CountMinAnyTypes type) : base(name, fieldName, type)
            {
            }
        }

        public enum FirstLastTypes
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

        public sealed class FirstLast : WithEnumType<FirstLastTypes>
        {
            public FirstLast(string name, string fieldName, string timeColumn, FirstLastTypes type) : base(name, fieldName, type)
            {
                TimeColumn = timeColumn;
            }

            public string TimeColumn { get; }
        }
    }
}
