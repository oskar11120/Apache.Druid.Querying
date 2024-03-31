using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;

namespace Apache.Druid.Querying.Internal
{
    public sealed record PropertyColumnNameMapping(string Property, string ColumnName)
    {
        public interface IProvider
        {
            ImmutableArray<PropertyColumnNameMapping> Get<TModel>();
            string GetColumnName(Type modelType, string propertyName);
        }

        public sealed class ImmutableBuilder : IProvider
        {
            public static ImmutableBuilder Create<TFirstModel>()
                => new ImmutableBuilder().Add<TFirstModel>();

            public ImmutableDictionary<Type, ImmutableArray<PropertyColumnNameMapping>> All { get; private set; }

            private ImmutableBuilder(ImmutableDictionary<Type, ImmutableArray<PropertyColumnNameMapping>>? all = null)
                => All = all ?? ImmutableDictionary<Type, ImmutableArray<PropertyColumnNameMapping>>.Empty;

            public ImmutableArray<PropertyColumnNameMapping> Get<TModel>() => Get(typeof(TModel));
            public ImmutableArray<PropertyColumnNameMapping> Get(Type modelType)
                => All.TryGetValue(modelType, out var result) ?
                    result :
                    ImmutableArray<PropertyColumnNameMapping>.Empty;

            public string GetColumnName(Type modelType, string propertyName)
                => Get(modelType)
                .FirstOrDefault(mapping => mapping.Property == propertyName)
                ?.ColumnName
                ?? propertyName;

            public ImmutableBuilder Add<TModel>()
            {
                var @new = Create(typeof(TModel));
                return new(All.AddRange(@new));
            }

            public ImmutableBuilder Add<TModel>(ImmutableArray<PropertyColumnNameMapping> mappings)
                => new(All.Add(typeof(TModel), mappings));

            public ImmutableBuilder Update<TModel>(Func<PropertyColumnNameMapping, PropertyColumnNameMapping> update)
            {
                var type = typeof(TModel);
                if (!All.TryGetValue(type, out var existing))
                {
                    return new(All);
                }

                var result = type
                    .GetGenericArguments()
                    .Append(type)
                    .Aggregate(All, (all, type) =>
                    {
                        if (!all.TryGetValue(type, out var forType))
                            forType = ImmutableArray<PropertyColumnNameMapping>.Empty;
                        var @new = forType.Select(update).ToImmutableArray();
                        return all.Remove(type).Add(type, @new);
                    });
                return new(result);
            }

            public ImmutableBuilder Combine(ImmutableBuilder other)
                => new(All.AddRange(other.All));

            private ImmutableDictionary<Type, ImmutableArray<PropertyColumnNameMapping>> Create(Type type)
            {
                var empty = ImmutableDictionary<Type, ImmutableArray<PropertyColumnNameMapping>>.Empty;
                if (All.TryGetValue(type, out var result))
                    return empty.Add(type, result);

                if (type.Namespace?.StartsWith("System", StringComparison.InvariantCulture) is true)
                    return empty;

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
                result = normal
                    .Concat(@explicitlyDeclared)
                    .Select(pair => new PropertyColumnNameMapping(
                        pair.name,
                        pair.property.GetCustomAttribute<DataSourceColumnAttribute>(true)?.Name ?? convention.Apply(pair.name)))
                    .Distinct()
                    .ToImmutableArray();
                var mappedIntoMultipleColumns = result
                    .GroupBy(mapping => mapping.Property)
                    .Select(group => (count: group.Count(), property: group.Key, mappings: group.AsEnumerable()))
                    .Where(count => count.count > 1);
                if (mappedIntoMultipleColumns.Any())
                    throw new InvalidOperationException($"At least one property of {type} has been mapped into multiple various columns.")
                    { Data = { [nameof(mappedIntoMultipleColumns)] = mappedIntoMultipleColumns } };

                var cululative = ImmutableArray<PropertyColumnNameMapping>.Empty;
                var arguments = type.GetGenericArguments();
                var agumentResults = arguments.Aggregate(empty, (result, type) => empty.AddRange(Create(type)));
                result = arguments.Length is 0 ?
                    result :
                    result
                        .Concat(arguments.SelectMany(type =>
                            agumentResults.TryGetValue(type, out var argumentResult) ?
                               argumentResult :
                               ImmutableArray<PropertyColumnNameMapping>.Empty))
                        .ToImmutableArray();
                return agumentResults.Add(type, result);
            }
        }
    }
}
