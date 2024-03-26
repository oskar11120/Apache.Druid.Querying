using System.Collections.Generic;

namespace Apache.Druid.Querying;

public sealed record SegmentMetadata(
    string Id,
    Dictionary<string, SegmentMetadata.Column> Columns,
    long NumRows,
    long? Size,
    Interval[]? Intervals,
    SegmentMetdataTimestampSpec? TimestampSpec,
    Dictionary<string, SegmentMetadata.Aggregator>? Aggregators,
    Granularity? QueryGranularity,
    bool? Rollup)
{
    public sealed record Column(
        string TypeSignature,
        string Type,
        bool HasMultipleValues,
        bool HasNulls,
        long? Size,
        string? MinValue,
        string? MaxValue,
        long? Cardinality,
        string? ErrorMessage);

    public sealed record Aggregator(string Type, string Name, string FieldName);

    public enum AnalysisType
    {
        Cardinality,
        Minmax,
        Size,
        Interval,
        TimestampSpec,
        QueryGranularity,
        Aggregators,
        Rollup
    }

    public enum AggregatorMergeStrategy
    {
        Strict,
        Lenient,
        Earliest,
        Latest
    }
}

public sealed record SegmentMetdataTimestampSpec(string Column, string Format, string? MissingValue);


