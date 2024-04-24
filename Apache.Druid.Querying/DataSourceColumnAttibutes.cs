using Apache.Druid.Querying.Internal;
using System;

namespace Apache.Druid.Querying
{
    [AttributeUsage(AttributeTargets.Property)]
    public class DataSourceColumnSelectorAttribute : Attribute
    {
        public DataSourceColumnSelectorAttribute(string forColumnMatchingProperty)
        {
            PropertyName = forColumnMatchingProperty;
        }

        public string PropertyName { get; }
    }

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

    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
    public abstract class DataSourceColumnNamingConvention : Attribute
    {
        public abstract string Apply(string memberName);

        public sealed class CamelCase : DataSourceColumnNamingConvention
        {
            public override string Apply(string memberName) => memberName.ToCamelCase();
        }

        internal sealed class None : DataSourceColumnNamingConvention
        {
            public static readonly None Singleton = new();

            public override string Apply(string memberName) => memberName;
        }
    }
}
