using System.Collections.Generic;

namespace Apache.Druid.Querying.Internal.QuerySectionFactory
{
    internal abstract class PostAggregator : WithType
    {
        public PostAggregator(string Name, string? type = null) : base(type)
        {
            this.Name = Name;
        }

        public string Name { get; }

        public class WithFields : PostAggregator
        {
            public WithFields(string name, IEnumerable<PostAggregator> fields, string? type = null) : base(name, type)
            {
                Fields = fields;
            }

            public IEnumerable<PostAggregator> Fields { get; }
        }

        public sealed class Arithmetic : WithFields
        {
            private static readonly Dictionary<ArithmeticFunction, string> map = new()
            {
                [ArithmeticFunction.Add] = "+",
                [ArithmeticFunction.Subtract] = "-",
                [ArithmeticFunction.Multiply] = "*",
                [ArithmeticFunction.Divide] = "/",
                [ArithmeticFunction.QuotientDivide] = "quotient",
                [ArithmeticFunction.Exponentiate] = "pow",
            };

            public Arithmetic(string name, IEnumerable<PostAggregator> fields, ArithmeticFunction fn) : base(name, fields)
            {
                Fn = map[fn];
            }

            public string Fn { get; }
        }

        public sealed class FieldAccess : PostAggregator
        {
            public FieldAccess(string name, string fieldName, bool finalizing) : base(name, finalizing ? "finalizingFieldAccess" : "fieldAccess")
            {
                FieldName = fieldName;
            }

            public string FieldName { get; }
        }

        public sealed class Constant<TValue> : PostAggregator
        {
            public Constant(string name, TValue value) : base(name)
            {
                Value = value;
            }

            public TValue Value { get; }
        }

        public sealed class Expression_ : PostAggregator
        {
            public Expression_(string name, string outputType, string expression) : base(name, nameof(expression))
            {
                OutputType = outputType;
                Expression = expression;
            }

            public string OutputType { get; }
            public string Expression { get; }
        }
    }
}
