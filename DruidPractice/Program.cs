// See https://aka.ms/new-console-template for more information
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Raygun.Druid4Net;
using Raygun.Druid4Net.Fluent.Filters;
using System.Buffers.Text;
using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Globalization;

Console.WriteLine("Hello, World!");

var options = new ConfigurationOptions
{
    QueryApiBaseAddress = new Uri("https://druid.emesh03.internal.digitalenterpriseconnect.com/"),
    JsonSerializer = new Serializer()
};
var t = DateTime.Parse("2023-10-15T16:57:00.000Z", null, DateTimeStyles.AssumeUniversal).ToUniversalTime();
//var t1 = DateTime.Parse("2023-11-01T17:57:00.000Z", null, DateTimeStyles.AssumeUniversal).ToUniversalTime();
var client = new DruidClient(options);
//var results = await client.TimeseriesAsync<JsonElement>(with => with
//    .Interval(t, DateTime.MaxValue)
//    .Granularity(Granularities.Minute)
//    .DataSource("data-variables")
//    .Filter(new AndFilter(
//        new SelectorFilter("variable", "pmPAct"),
//        new SelectorFilter("tenantId", "55022f5d-d9c4-4773-86e5-fbce823cd287"),
//        new SelectorFilter("objectId", "4460391b-b713-44eb-b422-2dbe7de91856")))
//    .Aggregations(
//        new DoubleSumAggregator("sum", "value"),
//        new CountAggregator("count"),
//        new StringFirstAggregator("variable", "variable"),
//        new StringFirstAggregator("first", "value"))
//    .PostAggregations(
//        new ArithmeticPostAggregator(
//            "average",
//            ArithmeticFunction.Divide,
//            null,
//            new FieldAccessPostAggregator("sum", "sum"),
//            new FieldAccessPostAggregator("count", "count")))
//    .Context(skipEmptyBuckets: true));

var results = await client.GroupByAsync<Result>(with => with
    .Interval(t, DateTime.MaxValue)
    .DataSource("data-variables")
    .Dimensions("objectId", "variable")
    .VirtualColumns(new[]
    {
        new ExpressionVirtualColumn(
            "tReal",
            "__time",
            ExpressionOutputType.LONG),
        new ExpressionVirtualColumn(
            "t",
            "timestamp(processedTimestamp)",
            ExpressionOutputType.LONG)
    })
    .Aggregations(
        new LongMaxAggregator("tMax", "t"),
        new ActualStringLastAggregator("value", "value", "t"))
    .Filter(new AndFilter(
        new OrFilter(
            new SelectorFilter("objectId", "0c8e5828-4e40-43c1-83d1-a3a6ba309866"),
            new SelectorFilter("variable", "priceFrcst")),
        new SelectorFilter("tenantId", "958b85b3-e45f-454e-81ad-3f9edec557ec")))
    .Granularity(Granularities.Hour)
    .Context(finalize: true, useCache: false));


var v = results
    .Data
    .GroupBy(result => result.Timestamp);

var c = v;

record ColumnCompareFilter(params string[] Dimensions) : IFilterSpec
{
    public string Type => "columnComparison";
}

class ActualStringLastAggregator : StringLastAggregator
{
    public string TimeColumn { get; }

    public ActualStringLastAggregator(string name, string fieldName, string timeColumn) : base(name, fieldName)
    {
        TimeColumn = timeColumn;
    }
}

public class MilisecondsConverter : JsonConverter<DateTimeOffset>
{
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var number = reader.TokenType switch
        {
            JsonTokenType.Number => reader.GetInt64(),
            JsonTokenType.String => long.Parse(reader.GetString()!),
            _ => throw new NotSupportedException()
        };
        return DateTimeOffset.UnixEpoch.AddMilliseconds(number);
    }

    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
    {
        // The "R" standard format will always be 29 bytes.
        Span<byte> utf8Date = stackalloc byte[29];

        bool result = Utf8Formatter.TryFormat(value, utf8Date, out _, new StandardFormat('R'));
        Debug.Assert(result);

        writer.WriteStringValue(utf8Date);
    }
}

class Serializer : IJsonSerializer
{
    private static readonly JsonSerializerOptions options = new(JsonSerializerDefaults.Web) { WriteIndented = true, Converters = { new MilisecondsConverter() } };

    public TResponse Deserialize<TResponse>(string responseData) => JsonSerializer.Deserialize<TResponse>(
        responseData, options)!;

    public string Serialize<TRequest>(TRequest request)
    {
        var temp = Newtonsoft.Json.JsonConvert.SerializeObject(
            request,
            Newtonsoft.Json.Formatting.Indented,
            new Newtonsoft.Json.JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver(), Converters = { new StringEnumConverter() } });
        return temp;
    }
}

record Result(
    [property: JsonPropertyName("tMax")] DateTimeOffset MaxProcessedTimestamp,
    [property: JsonPropertyName("tReal")] DateTimeOffset Timestmap,
    string Variable,
    Guid ObjectId,
    double Value);

record Message(string Variable, Guid ObjectId, double Value, DateTimeOffset Timestamp, DateTimeOffset ProcessedTimestmap);