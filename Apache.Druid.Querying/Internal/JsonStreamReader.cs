using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Druid.Querying.Internal;

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

    public Utf8JsonReader GetReaderForSlice(int sliceLengthInBytes, int trimStart)
    {
        var consumed = (int)_bytesConsumed;
        ReadOnlySpan<byte> slice = _buffer.AsSpan()[(consumed + trimStart)..(consumed + sliceLengthInBytes)];
        return new Utf8JsonReader(slice, false, default);
    }

    public Utf8JsonReader GetReader()
    {
        ReadOnlySpan<byte> slice = _bytesConsumed > 0 || _readCount < Size ? _buffer.AsSpan()[(int)_bytesConsumed.._readCount] : _buffer;
        var reader = new Utf8JsonReader(slice, false, _readerState);
        return reader;
    }

    public string DebugString => Debug();

    private string Debug()
    {
        ReadOnlySpan<byte> slice = _bytesConsumed > 0 || _readCount < Size ? _buffer.AsSpan()[(int)_bytesConsumed.._readCount] : _buffer;
        return Encoding.UTF8.GetString(slice);
    }


    public int Depth => _depth;

    public static int Size => 4 * 1024;

    public async Task AdvanceAsync(CancellationToken token)
    {
        // Save off existing text
        int leftoverLength = FlipBuffer();

        // Read from stream to fill remainder of buffer
        int read = await _stream.ReadAsync(_buffer.AsMemory()[leftoverLength..], token);
        _readCount = read + leftoverLength;
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
        {
            UpdateState(reader);
        }

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

    public bool ReadToTokenTypeAtNextTokenDepth(JsonTokenType tokenType, out long bytesConsumed, bool updateState = true)
    {
        bytesConsumed = default;
        var reader = GetReader();
        if (!reader.Read())
            return false;

        var depth = reader.CurrentDepth;
        while (true)
        {
            var read = ReadToTokenType(ref reader, tokenType, false);
            if (!read)
                return false;

            if (reader.CurrentDepth != depth)
                continue;

            if (updateState)
                UpdateState(reader);
            bytesConsumed = reader.BytesConsumed;
            return true;
        }
    }

    public bool ReadToTokenType(JsonTokenType tokenType, bool updateState = true)
    {
        var reader = GetReader();
        return ReadToTokenType(ref reader, tokenType, updateState);
    }

    public bool ReadToTokenType(ref Utf8JsonReader reader, JsonTokenType tokenType, bool updateState = true)
    {
        var found = false;

        while (reader.Read())
        {
            if (reader.TokenType == tokenType)
            {
                found = true;
                break;
            }
        }

        if (updateState)
        {
            UpdateState(reader);
        }

        return found;
    }

    public bool ReadToProperty(ReadOnlySpan<byte> name, bool updateState = true)
    {
        var reader = GetReader();
        var found = ReadToProperty(ref reader, name);

        if (updateState)
        {
            UpdateState(reader);
        }

        return found;
    }

    public bool ReadToPropertyValue<T>(ReadOnlySpan<byte> name, [NotNullWhen(true)] out T value, bool updateState = true)
    {
        var reader = GetReader();
        var found = ReadToProperty(ref reader, name);
        value = default!;

        if (found && reader.Read())
        {
            var type = typeof(T);

            if (type == typeof(bool))
            {
                var @bool = reader.GetBoolean();
                value = Unsafe.As<bool, T>(ref @bool);
            }
            else if (type == typeof(string))
            {
                var @string = reader.GetString() ?? throw new Exception("BAD JSON");
                value = Unsafe.As<string, T>(ref @string);
            }
            else if (type == typeof(DateTimeOffset))
            {
                var t = reader.GetDateTimeOffset();
                value = Unsafe.As<DateTimeOffset, T>(ref t);
            }
            else
            {
                throw new Exception("Unsupported type");
            }

        }
        else if (found)
        {
            found = false;
        }

        if (updateState)
        {
            UpdateState(reader);
        }

        return found;
    }

    public static bool ReadToProperty(ref Utf8JsonReader reader, ReadOnlySpan<byte> name)
    {
        var found = false;

        while (reader.Read())
        {
            if (reader.TokenType is JsonTokenType.PropertyName &&
                reader.ValueTextEquals(name))
            {
                found = true;
                break;
            }
        }

        return found;
    }
}