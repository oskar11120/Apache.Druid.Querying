using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Druid.Querying.Internal.Json;

internal sealed class UnexpectedEndOfStreamException : JsonException
{
    public UnexpectedEndOfStreamException() : base("Reached end of stream before finishing deserialization.")
    {          
    }
}

// Based on https://github.com/richlander/convenience/blob/main/releasejson/releasejson/JsonStreamReader.cs.
internal sealed class JsonStreamReader
{
    public JsonStreamReader(Stream stream, byte[] buffer, int readCount)
    {
        _stream = stream;
        _buffer = buffer;
        _readCount = readCount;
    }

    private readonly Stream _stream;
    private readonly byte[] _buffer;
    private JsonReaderState _readerState = default;
    private int _depth = 0;
    private long _bytesConsumed = 0;
    private int _readCount;
    public JsonTokenType TokenType { get; private set; } = JsonTokenType.None;

    public void UpdateState(Utf8JsonReader reader)
    {
        _bytesConsumed += reader.BytesConsumed;
        _readerState = reader.CurrentState;
        _depth = reader.CurrentDepth;
        TokenType = reader.TokenType;
    }

    public ReadOnlySpan<byte> GetSpan() => _bytesConsumed > 0 || _readCount < Size ? _buffer.AsSpan()[(int)_bytesConsumed.._readCount] : _buffer;

    public Utf8JsonReader GetReader() => new(GetSpan(), false, _readerState);

    internal string DebugView => Encoding.UTF8.GetString(GetSpan());

    public int Depth => _depth;

    public static int Size => 4 * 1024;

    public async Task AdvanceAsync(CancellationToken token)
    {
        // Save off existing text
        int leftoverLength = FlipBuffer();
        if (leftoverLength <= 0 && _readCount != Size)
            throw new InvalidOperationException($"Buffer full. Content: \n{DebugView}");

        // Read from stream to fill remainder of buffer
        int read = await _stream.ReadAsync(_buffer.AsMemory()[leftoverLength..], token);
        _readCount = read + leftoverLength;
        if (read == 0)
            throw new UnexpectedEndOfStreamException();
    }

    private int FlipBuffer()
    {
        var buffer = _buffer.AsSpan();
        var text = buffer[(int)_bytesConsumed.._readCount];
        text.CopyTo(buffer);
        _bytesConsumed = 0;
        return text.Length;
    }

    public async ValueTask<JsonTokenType> ReadNextAsync(CancellationToken token, bool updateState = true)
    {
        bool Try(out JsonTokenType type)
        {
            var reader = GetReader();
            if (reader.Read())
            {
                type = reader.TokenType;
                if (updateState)
                    UpdateState(reader);
                return true;
            }

            type = default;
            return false;
        }

        JsonTokenType type;
        while (!Try(out type))
            await AdvanceAsync(token);
        return type;
    }

    public async ValueTask ReadToTokenAsync(JsonTokenType ofType, CancellationToken token, bool updateState = true)
    {
        bool Try()
        {
            var reader = GetReader();
            var found = reader.ReadToToken(ofType);
            if (updateState)
                UpdateState(reader);
            return found;
        }

        while (!Try())
            await AdvanceAsync(token);
    }

    public async ValueTask<long> ReadToTokenAsync(JsonTokenType ofType, int atDepth, CancellationToken token, bool updateState = true)
    {
        bool Try(out long consumed)
        {
            var reader = GetReader();
            var found = reader.ReadToToken(ofType, atDepth);
            if (updateState)
                UpdateState(reader);
            consumed = reader.BytesConsumed;
            return found;
        }

        long consumed;
        while (!Try(out consumed))
            await AdvanceAsync(token);
        return consumed;
    }

    public async ValueTask ReadToPropertyAsync(ReadOnlyMemory<byte> name, CancellationToken token, bool updateState = true)
    {
        bool Try()
        {
            var reader = GetReader();
            var found = reader.ReadToProperty(name.Span);
            if (updateState)
                UpdateState(reader);
            return found;
        }

        while (!Try())
            await AdvanceAsync(token);
    }

    public async ValueTask<TValue> ReadToPropertyValueAsync<TValue>(ReadOnlyMemory<byte> name, CancellationToken token, bool updateState = true)
    {
        bool Try(out TValue value)
        {
            var reader = GetReader();
            var found = reader.ReadToPropertyValue(name.Span, out value);
            if (updateState)
                UpdateState(reader);
            return found;
        }

        TValue value;
        while (!Try(out value))
            await AdvanceAsync(token);
        return value;
    }
}