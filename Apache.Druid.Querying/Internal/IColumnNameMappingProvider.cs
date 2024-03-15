using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;

namespace Apache.Druid.Querying.Internal
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

            public ImmutableBuilder Add<TModel>()
            {
                var type = typeof(TModel);
                var convention = type
                    .GetCustomAttribute<DataSourceColumnNamingConvention>()
                    ?? DataSourceColumnNamingConvention.None.Singleton;
                var @explicitlyDeclared = type
                    .GetProperties(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                    .Where(property => property.GetGetMethod(true) is { IsFinal: true, IsPrivate: true })
                    .Select(property => (property, name: property.Name[(property.Name.LastIndexOf('.') + 1)..]));
                var normal = type
                    .GetProperties()
                    .Select(property => (property, name: property.Name));
                var result = normal
                    .Concat(@explicitlyDeclared)
                    .Select(pair => new PropertyColumnNameMapping(
                        pair.name,
                        pair.property.GetCustomAttribute<DataSourceColumnAttribute>(true)?.Name ?? convention.Apply(pair.name)))
                    .Where(mapping => mapping.Property != mapping.ColumnName)
                    .Distinct()
                    .ToImmutableArray();
                var mappedIntoMultipleColumns = result
                    .GroupBy(mapping => mapping.Property)
                    .Select(group => (count: group.Count(), property: group.Key, mappings: group.AsEnumerable()))
                    .Where(count => count.count > 1);
                if (mappedIntoMultipleColumns.Any())
                    throw new InvalidOperationException($"At least one property of {typeof(TModel)} has been mapped into multiple various columns.")
                    { Data = { [nameof(mappedIntoMultipleColumns)] = mappedIntoMultipleColumns } };
                return new(All = All.Add(type, result));
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
