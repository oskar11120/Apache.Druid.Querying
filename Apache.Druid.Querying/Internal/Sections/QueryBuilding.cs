using System.Linq;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Internal.Sections
{
    public interface IQueryWithSectionFactoryExpressions
    {
        SectionAtomicity.IProvider.Builder SectionAtomicity { get; }
    }

    // TMarker is a workaround to https://learn.microsoft.com/en-us/dotnet/csharp/misc/cs0695?f1url=%3FappId%3Droslyn%26k%3Dk(CS0695).
    public interface IQueryWithSectionFactoryExpressions<TArguments, TSelf, TMarker> :
        IQueryWithSectionFactoryExpressions,
        IQuery<TSelf>
        where TSelf : IQuery<TSelf>
    {
    }

    internal static class QueryExtensions
    {
        public static TSelf AddOrUpdateSectionWithSectionFactory<TArguments, TSelf, TMarker, TSection, TElementFactory>(
            this IQueryWithSectionFactoryExpressions<TArguments, TSelf, TMarker> query,
            string atomicSectionColumnName,
            Expression<QuerySectionFactory<TElementFactory, TSection>> factory,
            SectionFactoryJsonMapper.Options? mapperOptions = null,
            bool convertKeyToCamelCase = true)
            where TSelf : IQuery<TSelf>
        {
            var calls = SectionFactoryParser
                .Execute(
                    factory,
                    typeof(TElementFactory),
                    typeof(TArguments),
                    typeof(TSection))
                .ToList();
            var atomicity = query.SectionAtomicity.Add<TSection>(calls, atomicSectionColumnName);
            return query.AddOrUpdateSection(
                atomicSectionColumnName,
                (options, columnNames) => SectionFactoryJsonMapper.Map(
                     calls, atomicity, options, columnNames, mapperOptions ?? SectionFactoryJsonMapper.Options.Default),
                convertKeyToCamelCase);
        }
    }
}
