using System.Collections.Generic;
using System.Linq;
using System;

namespace Apache.Druid.Querying.Internal;
internal static class GetGenericInterfacesTypeExtension
{
    internal static IEnumerable<Type> GetGenericInterfaces(this IQueryWith.State query, Type matchingType) => query
        .GetType()
        .GetInterfaces()
        .Where(@interface => @interface.IsGenericType && @interface.GetGenericTypeDefinition() == matchingType);
}
