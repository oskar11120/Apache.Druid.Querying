using System;

namespace Apache.Druid.Querying
{
    [AttributeUsage(AttributeTargets.Property)]
    public class DataSourceColumn : Attribute
    {
        public DataSourceColumn(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class DataSourceTimeColumn : DataSourceColumn
    {
        public new static readonly string Name = "__time";

        public DataSourceTimeColumn() : base(Name)
        {
        }
    }
}
