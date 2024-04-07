using System.Text.Json;

namespace Apache.Druid.Querying.Json;

public sealed class UnexpectedEndOfJsonStreamException : JsonException
{
    public UnexpectedEndOfJsonStreamException() : base("Reached the end of stream before finishing deserialization.")
    {
    }
}