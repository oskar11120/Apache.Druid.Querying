using System.Linq.Expressions;

namespace Apache.Druid.Querying.Internal.Sections
{
    // TMarker is a workaround to https://learn.microsoft.com/en-us/dotnet/csharp/misc/cs0695?f1url=%3FappId%3Droslyn%26k%3Dk(CS0695).
    public interface IQuery<TArguments, TSelf, TMarker> : IQuery<TSelf> where TSelf : IQuery<TSelf>
    {
    }

    internal static class QueryExtensions
    {
        public static TSelf AddOrUpdateSectionWithSectionFactory<TArguments, TSelf, TMarker, TSection, TElementFactory>(
            this IQuery<TArguments, TSelf, TMarker> query,
            string key,
            Expression<QuerySectionFactory<TElementFactory, TSection>> factory,
            SectionFactoryJsonMapper.CustomMappings? customMappings = null,
            bool convertKeyToCamelCase = true)
            where TSelf : IQuery<TSelf>
            => query.AddOrUpdateSection(
                key,
                options => SectionFactoryJsonMapper.Map(factory, typeof(TArguments), key, options, customMappings),
                convertKeyToCamelCase);
    }
}
