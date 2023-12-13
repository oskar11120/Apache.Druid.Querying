
namespace Apache.Druid.Querying
{
    public abstract class VirtualColumn : WithType
    {
        public sealed class Expression_ : VirtualColumn
        {
            public Expression_(string name, string expression, string outputType)
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
