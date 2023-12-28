using System;
using System.Linq.Expressions;

namespace Apache.Druid.Querying.Internal.Sections
{
    internal static class QueryExtensions
    {
        public static TSelf AddOrUpdateSection<TSelf, TElementFactory, TSection>(
            this IQuery<TSelf> query,
            string key,
            Type argumentsType,
            Expression<QuerySectionFactory<TElementFactory, TSection>> factory,
            SectionFactoryJsonMapper.CustomMappings? customMappings = null,
            bool convertKeyToCamelCase = true)
            where TSelf : IQuery<TSelf>
            => query.AddOrUpdateSection(
                key,
                options => SectionFactoryJsonMapper.Map(factory, argumentsType, options, customMappings),
                convertKeyToCamelCase);
    }
}
