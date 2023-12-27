namespace Apache.Druid.Querying.Internal.QuerySectionFactory
{
    public abstract class Dimension : WithType
    {
        public Dimension(string? type = null) : base(type)
        {
        }

        public class Default : Dimension
        {
            public Default(string dimension, string outputName, SimpleDataType outputType, string? type = null) : base(type)
            {
                Dimension = dimension;
                OutputName = outputName;
                OutputType = outputType;
            }

            public string Dimension { get; }
            public string OutputName { get; }
            public SimpleDataType OutputType { get; }
        }

        public sealed class Extraction : Default
        {
            public Extraction(string dimension, string outputName, SimpleDataType outputType, string extractionFn)
                : base(dimension, outputName, outputType)
            {
                ExtractionFn = extractionFn;
            }

            // TODO
            public string ExtractionFn { get; }
        }
    }
}
