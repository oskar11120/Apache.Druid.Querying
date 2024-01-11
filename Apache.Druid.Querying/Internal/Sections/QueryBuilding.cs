using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace Apache.Druid.Querying.Internal.Sections
{
    // TMarker is a workaround to https://learn.microsoft.com/en-us/dotnet/csharp/misc/cs0695?f1url=%3FappId%3Droslyn%26k%3Dk(CS0695).
    public interface IQuery<TArguments, TSelf, TMarker> : IQuery<TSelf> where TSelf : IQuery<TSelf>
    {
    }

    internal static class QueryExtensions
    {
        private static readonly ConditionalWeakTable<IQuery, SectionAtomicity.IProvider.Builder> builders = new();

        private static SectionAtomicity.IProvider.Builder GetBuilder(this IQuery query)
            => builders.GetOrCreateValue(query);

        public static SectionAtomicity.IProvider GetSectionAtomicity(this IQuery query) => query.GetBuilder();

        public static TSelf AddOrUpdateSectionWithSectionFactory<TArguments, TSelf, TMarker, TSection, TElementFactory>(
            this IQuery<TArguments, TSelf, TMarker> query,
            string atomicSectionColumnName,
            Expression<QuerySectionFactory<TElementFactory, TSection>> factory,
            SectionFactoryJsonMapper.Options? mapperOptions = null,
            bool convertKeyToCamelCase = true)
            where TSelf : IQuery<TSelf>
        {
            var calls = SectionFactoryInterpreter
                .Execute(
                    factory,
                    typeof(TElementFactory),
                    typeof(TArguments))
                .ToList();
            var atomicity = query.GetBuilder().Add<TSection>(calls, atomicSectionColumnName);
            return query.AddOrUpdateSection(
                atomicSectionColumnName,
                (options, columnNames) => SectionFactoryJsonMapper.Map(
                     calls, atomicity, options, columnNames, mapperOptions ?? SectionFactoryJsonMapper.Options.Default),
                convertKeyToCamelCase);
        }
    }
}
