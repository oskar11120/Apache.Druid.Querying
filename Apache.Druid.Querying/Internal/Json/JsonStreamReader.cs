using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Druid.Querying.Internal.Json;

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

    public void UpdateState(Utf8JsonReader reader)
    {
        _bytesConsumed += reader.BytesConsumed;
        _readerState = reader.CurrentState;
        _depth = reader.CurrentDepth;
    }

    public bool UnreadPartOfBufferStartsWith(ReadOnlySpan<byte> bytes)
    {
        var consumed = (int)_bytesConsumed;
        return bytes.SequenceEqual(_buffer.AsSpan()[consumed..(consumed + bytes.Length)]);
    }

    public ReadOnlySpan<byte> GetSliceOfBuffer(int sliceLengthInBytes, int trimStart)
    {
        var consumed = (int)_bytesConsumed;
        var slice = _buffer.AsSpan()[(consumed + trimStart)..(consumed + sliceLengthInBytes)];
        return slice;
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
        if (leftoverLength <= 0)
            throw new InvalidOperationException("Buffer full.");

        // Read from stream to fill remainder of buffer
        int read = await _stream.ReadAsync(_buffer.AsMemory()[leftoverLength..], token);
        _readCount = read + leftoverLength;
        if (read == 0)
            throw new InvalidOperationException("Reached end of the stream.");
    }

    private int FlipBuffer()
    {
        var buffer = _buffer.AsSpan();
        var text = buffer[(int)_bytesConsumed.._readCount];
        text.CopyTo(buffer);
        _bytesConsumed = 0;
        return text.Length;
    }

    public bool ReadToDepth(int depth, bool updateState = true)
    {
        var reader = GetReader();
        var found = false;

        while (reader.Read())
        {
            if (reader.CurrentDepth <= depth)
            {
                found = true;
                break;
            }
        }

        if (updateState)
            UpdateState(reader);
        return found;
    }

    public bool ReadNext([NotNullWhen(true)] out JsonTokenType tokenType)
    {
        var reader = GetReader();

        if (reader.Read())
        {
            tokenType = reader.TokenType;
            return true;
        }

        tokenType = default;
        return false;
    }

    public async ValueTask<long> AdvanceTillAllOfGreaterThanCurrentDepthInBufferAsync(CancellationToken token)
    {
        bool TryRead(out long consumed)
        {
            var reader = GetReader();
            if (reader.ReadThroughAllOfGreaterThanCurrentDepth())
            {
                consumed = reader.BytesConsumed;
                return true;
            }

            consumed = default;
            return false;
        }

        long consumed;
        while (!TryRead(out consumed))
            await AdvanceAsync(token);
        return consumed;
    }

    public bool ReadToToken(JsonTokenType ofType, bool updateState = true)
    {
        var reader = GetReader();
        return ReadToToken(ref reader, ofType, updateState);
    }

    public bool ReadToToken(ref Utf8JsonReader reader, JsonTokenType ofType, bool updateState = true)
    {
        var found = reader.ReadToToken(ofType);
        if (updateState)
            UpdateState(reader);
        return found;
    }

    public bool ReadToProperty(ReadOnlySpan<byte> name, bool updateState = true)
    {
        var reader = GetReader();
        var found = reader.ReadToProperty(name);
        if (updateState)
            UpdateState(reader);
        return found;
    }

    public bool ReadToPropertyValue<T>(ReadOnlySpan<byte> name, [NotNullWhen(true)] out T value, bool updateState = true)
    {
        var reader = GetReader();
        var found = reader.ReadToPropertyValue(name, out value);
        if (updateState)
            UpdateState(reader);
        return found;
    }
}