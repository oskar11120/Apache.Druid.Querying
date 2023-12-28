using Apache.Druid.Querying.Internal;
using System;
using System.Threading;

namespace Apache.Druid.Querying
{
    [AttributeUsage(AttributeTargets.Property)]
    public class DataSourceColumnAttribute : Attribute
    {
        public DataSourceColumnAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class DataSourceTimeColumnAttribute : DataSourceColumnAttribute
    {
        public new static readonly string Name = "__time";

        public DataSourceTimeColumnAttribute() : base(Name)
        {
        }
    }

    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface)]
    public sealed class DataSourceColumnNamingConventionAttribute : Attribute
    {
        public DataSourceColumnNamingConventionAttribute(IDataSourceColumnNamingConvention convention)
        {
            Convention = convention;
        }

        public DataSourceColumnNamingConventionAttribute(DataSourceColumnNamingConventionType type)
        {
            Convention = type switch
            {
                DataSourceColumnNamingConventionType.CamelCase => CamelCase.Singleton,
                _ => throw new NotSupportedException(nameof(DataSourceColumnNamingConventionType))
            };
        }

        public IDataSourceColumnNamingConvention Convention { get; }

        private sealed class CamelCase : IDataSourceColumnNamingConvention
        {
            public static readonly CamelCase Singleton = new();

            public string Apply(string memberName) => memberName.ToCamelCase();
        }

    }

    public interface IDataSourceColumnNamingConvention
    {
        string Apply(string memberName);

        internal sealed class None : IDataSourceColumnNamingConvention
        {
            public static readonly None Singleton = new();

            public string Apply(string memberName) => memberName;
        }
    }

    public enum DataSourceColumnNamingConventionType
    {
        CamelCase
    }
}
