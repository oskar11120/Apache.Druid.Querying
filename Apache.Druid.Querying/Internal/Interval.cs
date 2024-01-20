using System.Globalization;
using System;

namespace Apache.Druid.Querying.Internal
{
    internal static class IntervalExtensions
    {
        public static string Map(this Interval interval)
        {
            static string ToIsoString(DateTimeOffset t) => t.ToString("o", CultureInfo.InvariantCulture);
            return $"{ToIsoString(interval.From)}/{ToIsoString(interval.To)}";
        }
    }
}
