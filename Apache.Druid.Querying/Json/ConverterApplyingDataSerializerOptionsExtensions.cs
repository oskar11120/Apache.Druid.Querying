using System.Text.Json;
using static Apache.Druid.Querying.Json.DefaultSerializerOptions.Data;

namespace Apache.Druid.Querying.Json;
internal static class ConverterApplyingDataSerializerOptionsExtensions
{
    public static JsonSerializerOptions SerializeDateTimeAndDateTimeOffsetAsUnixMiliseconds(this JsonSerializerOptions options)
    {
        var converters = options.Converters;
        converters.Remove(UnixMilisecondsAllowingConverter.OfDateTimeOffset.WritingIsoTimestamps.Singleton);
        converters.Remove(UnixMilisecondsAllowingConverter.OfDateTime.WritingIsoTimestamps.Singleton);
        converters.Add(UnixMilisecondsAllowingConverter.OfDateTimeOffset.WritingUnixMiliseconds.Singleton);
        converters.Add(UnixMilisecondsAllowingConverter.OfDateTime.WritingUnixMiliseconds.Singleton);
        return options;
    }

    public static JsonSerializerOptions SerializeBoolsAsNumbers(this JsonSerializerOptions options)
    {
        var converters = options.Converters;
        converters.Remove(NumberAndTextAllowingBoolConverter.WritingBools.Singleton);
        converters.Add(NumberAndTextAllowingBoolConverter.WritingNumbers.Singleton);
        return options;
    }
}
