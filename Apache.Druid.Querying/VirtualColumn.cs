
namespace Apache.Druid.Querying
{
    public abstract class VirtualColumn : WithType
    {
        public VirtualColumn(string? type = null) : base(type)
        {
        }

        public sealed class Expression_ : VirtualColumn
        {
            public Expression_(string name, string expression, string outputType) : base(nameof(expression))
            {
                Name = name;
                Expression = expression;
                OutputType = outputType;
            }

            public string Name { get; }
            public string Expression { get; }
            public string OutputType { get; }
        }
    }
}
