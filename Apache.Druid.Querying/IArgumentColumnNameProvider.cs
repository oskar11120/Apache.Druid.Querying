using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Apache.Druid.Querying
{
    public interface IArgumentColumnNameProvider
    {
        public string Get(string memberName);

        internal sealed class Implementation<TSource> : IArgumentColumnNameProvider
        {
            public static readonly Implementation<TSource> Singleton = new();

            private static readonly IReadOnlyDictionary<string, string> mappings = GetMappings();

            public static Dictionary<string, string> GetMappings()
            {
                var type = typeof(TSource);
                var convention = type.GetCustomAttribute<DataSourceColumnNamingConventionAttribute>()?.Convention
                    ?? IDataSourceColumnNamingConvention.None.Singleton;
                var properties = type.GetProperties();
                var result = properties.ToDictionary(
                    property => property.Name,
                    property => property.GetCustomAttribute<DataSourceColumnAttribute>(true)?.Name ?? convention.Apply(property.Name));
                var hasTimeColumn = result.Values.Any(name => name == DataSourceTimeColumnAttribute.Name);
                if (!hasTimeColumn)
                {
                    var timeProperties = properties
                        .Where(property =>
                            property.PropertyType == typeof(DateTime) ||
                            property.PropertyType == typeof(DateTimeOffset))
                        .ToArray();
                    var timeColumnName = timeProperties.Length is 1 ?
                        timeProperties[0].Name :
                        throw new InvalidOperationException(
                            $"Could not match any property of {type} with column {DataSourceTimeColumnAttribute.Name}. " +
                            $"A property is matched with column {DataSourceTimeColumnAttribute.Name} when either:" +
                            $"{Environment.NewLine}- it's decorated with {nameof(Attribute)} {typeof(DataSourceTimeColumnAttribute)}" +
                            $"{Environment.NewLine}- it's the only property of type {typeof(DateTime)} or {typeof(DateTimeOffset)}.");
                    result.Remove(timeColumnName);
                    result.Add(timeColumnName, DataSourceTimeColumnAttribute.Name);
                }

                return result;
            }

            public string Get(string memberName) => mappings.TryGetValue(memberName, out var result) ? result : memberName;
        }
    }
}
