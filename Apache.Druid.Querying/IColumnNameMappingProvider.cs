using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;

namespace Apache.Druid.Querying
{
    public sealed record PropertyColumnNameMapping(string Property, string ColumnName);

    public interface IColumnNameMappingProvider
    {
        IReadOnlyList<PropertyColumnNameMapping> Get<TModel>();
        string GetColumnName(Type modelType, string propertyName);

        internal sealed class ImmutableBuilder : IColumnNameMappingProvider
        {
            public static ImmutableBuilder Create<TFirstModel>()
                => new ImmutableBuilder().Add<TFirstModel>();

            public ImmutableDictionary<Type, ImmutableArray<PropertyColumnNameMapping>> All { get; private set; }

            public ImmutableBuilder(ImmutableDictionary<Type, ImmutableArray<PropertyColumnNameMapping>>? all = null)
                => All = all ?? ImmutableDictionary<Type, ImmutableArray<PropertyColumnNameMapping>>.Empty;

            public IReadOnlyList<PropertyColumnNameMapping> Get<TModel>() => Get(typeof(TModel));
            public IReadOnlyList<PropertyColumnNameMapping> Get(Type modelType)
                => All.TryGetValue(modelType, out var result) ?
                    result :
                    Array.Empty<PropertyColumnNameMapping>();

            public string GetColumnName(Type modelType, string propertyName)
                => Get(modelType)
                .FirstOrDefault(mapping => mapping.Property == propertyName)
                ?.ColumnName
                ?? propertyName;

            public ImmutableBuilder Add<TModel>(ImmutableArray<PropertyColumnNameMapping> mappings)
                => new(All = All.Add(typeof(TModel), mappings));

            public ImmutableBuilder Add<TModel>()
            {
                var type = typeof(TModel);
                var convention = type.GetCustomAttribute<DataSourceColumnNamingConventionAttribute>()
                    ?.Convention
                    ?? IDataSourceColumnNamingConvention.None.Singleton;
                var properties = type.GetProperties();
                var result = properties
                    .Select(property => new PropertyColumnNameMapping(
                        property.Name,
                        property.GetCustomAttribute<DataSourceColumnAttribute>(true)?.Name ?? convention.Apply(property.Name)))
                    .Where(mapping => mapping.Property != mapping.ColumnName)
                    .ToImmutableArray();
                return Add<TModel>(result);
            }

            public ImmutableBuilder Update<TModel>(Func<PropertyColumnNameMapping, PropertyColumnNameMapping> update)
            {
                var type = typeof(TModel);
                if (!All.TryGetValue(type, out var existing))
                {
                    throw new InvalidOperationException($"No mapping for type {type} exist.");
                }

                var @new = type
                    .GetProperties()
                    .Where(property => !existing.Any(mapping => mapping.Property == property.Name))
                    .Select(property => new PropertyColumnNameMapping(property.Name, property.Name))
                    .Concat(existing)
                    .Select(update)
                    .ToImmutableArray();
                var result = All.Remove(type);
                return new(All = result.Add(type, @new));
            }

            public ImmutableBuilder Combine(ImmutableBuilder other)
                => new(All.AddRange(other.All));
        }
    }
}
