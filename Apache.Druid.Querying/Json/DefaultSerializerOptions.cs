using System.Diagnostics.CodeAnalysis;
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Druid.Querying.Json
{
    public static class DefaultSerializerOptions
    {
        public static JsonSerializerOptions Create() => new(JsonSerializerDefaults.Web)
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            Converters =
            {
                new JsonStringEnumConverter(),
                new PolymorphicSerializer<IFilter>(),
                new PolymorphicSerializer<IMetric>(),
                new PolymorphicSerializer<IHaving>(),
                new PolymorphicSerializer<ILimitSpec>(),
                new PolymorphicSerializer<ILimitSpec.OrderBy>()
            }
        };

        private sealed class PolymorphicSerializer<T> : JsonConverter<T> where T : class
        {
            private JsonSerializerOptions? withoutPolymorphicSerializer;

            public override T Read(
                ref Utf8JsonReader reader,
                Type typeToConvert,
                JsonSerializerOptions options)
                => throw new NotSupportedException();

            public override void Write(
                Utf8JsonWriter writer,
                [DisallowNull] T value,
                JsonSerializerOptions options)
            {
                var type = value.GetType();
                if (type == typeof(T))
                {
                    if (withoutPolymorphicSerializer is null)
                    {

                        withoutPolymorphicSerializer = new(options);
                        withoutPolymorphicSerializer.Converters.Remove(this);
                    }

                    options = withoutPolymorphicSerializer;
                }

                JsonSerializer.Serialize(writer, value, type, options);
            }
        }
    }
}
