# [Apache Druid](http://druid.io/) client library/orm for dotnet 6+ inspired by [EF Core](https://learn.microsoft.com/pl-pl/ef/core/).

https://www.nuget.org/packages/Apache.Druid.Querying

## Setup
To make your Druid data sources available for querying create a class deriving from `Apache.Druid.Querying.DataSourceProvider`. The class represents collection of data sources available for querying similarly to how `EfCore`'s `DbContext` represents collection of database tables. The class contains methods `Table`, `Lookup` and `Inline` which you can use to create instances of `Apache.Druid.Querying.DataSource` (similar to `EfCore`'s `DbSet`) which in turn can be used of querying. The instances are thread-safe and so can be used for executing multiple queries at the same time. Some of the `DataSource` creating methods require parameter `id` which corresponds to id of related `Druid` data source.

The method `Table` additionally requires generic parameter `TSource` depicting a row of your table data, similarly to how `EfCore`'s `Entities` depict database rows. The type's public properties correspond to the data source columns.

By default `TSource` property names map 1-to-1 into `Druid` data source column names. This can be overridden in two ways:
- By decorating `TSource` with `Apache.Druid.Querying.DataSourceNamingConvention` attribute. The convention will applied to all `TSource`'s property names.
- By decorating `TSource`'s properties with `Apache.Druid.Querying.DataSourceColumn` attribute. The string parameter passed to the attribute will become the data source column name. As most `Druid` data sources contain column `__time` for convenience there exists attribute `Apache.Druid.Querying.DataSourceTimeColumn` equivalent to `Apache.Druid.Querying.DataSourceColumn("__time")`.

```cs
    [DataSourceColumnNamingConvention.CamelCase]
    public record Edit(
        [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
        bool IsRobot,
        string Channel,
        string Flags,
        bool IsUnpatrolled,
        string Page,
        [property: DataSourceColumn("diffUrl")] string DiffUri,
        int Added,
        string Comment,
        int CommentLength,
        bool IsNew,
        bool IsMinor,
        int Delta,
        bool IsAnonymous,
        string User,
        int DeltaBucket,
        int Deleted,
        string Namespace,
        string CityName,
        string CountryName,
        string? RegionIsoCode,
        int? MetroCode,
        string? CountryIsoCode,
        string? RegionName);

    public class WikipediaDataSourceProvider : DataSourceProvider
    {
        public WikipediaDataSourceProvider()
        {
            // Druid's example wikipedia edits data source.
            Edits = Table<Edit>("wikipedia");
        }

        public DataSource<Edit> Edits { get; }
    }
```

Then connect up your data source provider to a dependency injection framework of your choice:
- [Microsoft.Extensions.DependencyInjection](Apache.Druid.Querying.Microsoft.Extensions.DependencyInjection/README.md)

## Querying
Choose query type and models representing query's data using nested types of `Apache.Druid.Querying.Query<TSource>`. Create a query by instantiating chosen nested type. Set query data by calling the instance methods. The methods often accept `Expression<Delegate>`, using which given an object representing input data available at that point in a query and an object representing all possible operations on that input data, you create an object representing results of your chosen operations. To get an idea on what's possible it's best to look into project's tests.

Get query json representation (to be sent to druid upon query execution) by calling `Apache.Druid.Querying.DataSource<TSource>.MapQueryToJson`. Execute query by calling `Apache.Druid.Querying.DataSource<TSource>.ExecuteQuery`.

Available query types:
- TimeSeries
- TopN
- GroupBy
- Scan
- SegmentMetadata
- DataSourceMetadata

```cs
    // Getting DataSourceProvider from dependency injection container.
    private static WikipediaDataSourceProvider Wikipedia 
        => Services.GetRequiredService<WikipediaDataSourceProvider>();

    private record Aggregations(int Count, int TotalAdded);
    private record PostAggregations(double AverageAdded);
    public void ExampleTimeSeries()
    { 
        var query = new Query<Edit>
            .TimeSeries
            .WithNoVirtualColumns
            .WithAggregations<Aggregations>
            .WithPostAggregations<PostAggregations>()
            .Order(OrderDirection.Descending)
            .Aggregations(type => new Aggregations( // Explicitly stating data types in the methods for the sake of clarity in the example. Query is able to infer them.
                type.Count(),
                type.Sum((Edit edit) => edit.Added)))
            .PostAggregations(type => new PostAggregations(type.Arithmetic(
                ArithmeticFunction.Divide,
                type.FieldAccess(aggregations => aggregations.TotalAdded),
                type.FieldAccess(aggregations => aggregations.Count))))
            .Filter(type => type.Selector(edit => edit.CountryIsoCode, "US"))
            .Interval(new(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1)))
            .Granularity(Granularity.Hour)
            .Context(new QueryContext.TimeSeries() { SkipEmptyBuckets = true });
        var json = Wikipedia.Edits.MapQueryToJson(query); // Use MapQueryToJson to look up query's json representation.
        IAsyncEnumerable<WithTimestamp<Aggregations_PostAggregations<Aggregations, PostAggregations>>> results 
            = Wikipedia.Edits.ExecuteQuery(query);
    }
```

## Data types

In Apache Druid operations on data have multiple "variants". Which variant you may want to choose in which query depends on:

- Data type of column used in the operation.
- Expected result of the operation.

For example, to perform a sum over some column's values, you may use:

- doubleSum 
- floatSum 
- longSum.

Most often though, you want the operation to match your column's data type. For this reason, such operations have been "merged" into one, accepting optional parameter of type `SimpleDataType`. Given example of operation `Sum`:
<table>
<thead>
  <tr>
    <th>Apache.Druid.Querying</th>
    <th>Apache Druid</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>

```cs
query
    .Aggregations(type => new(
        type.Sum(edit => edit.Added, SimpleDataType.Double)));
```
</td>    
<td>

```json
{
    "aggregations": [
        {
          "type": "doubleSum",
          "name": "TotalAdded",
          "fieldName": "added"
        }
      ]
}
```
</td>
</tr>
<tr>
<td>

```cs
query
    .Aggregations(type => new(
        type.Sum(edit => edit.Added, SimpleDataType.Float)));
```
</td>
<td>

```json
{
    "aggregations": [
        {
          "type": "floatSum",
          "name": "TotalAdded",
          "fieldName": "added"
        }
      ]
}
```
</td>
</tr>
<tr>
<td>

```cs
query
    .Aggregations(type => new(
        type.Sum(edit => edit.Added, SimpleDataType.Long)));
```
</td>
<td>

```json
{
    "aggregations": [
        {
          "type": "longSum",
          "name": "TotalAdded",
          "fieldName": "added"
        }
      ]
}
```
</td>
</tr>
</tbody>
</table>

In case `SimpleDataType` has not been specified, the library will infer it from related property type with following logic:
<table>
<thead>
  <tr>
    <th>Property type</th>
    <th>Druid data type</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>string, Guid, char, Uri, Enum</td>
    <td>String</td>
  </tr>
  <tr>
    <td>double</td>
    <td>Double</td>
  </tr>
  <tr>
    <td>float</td>
    <td>Float</td>
  </tr>
  <tr>
    <td>short, int, long, DateTime, DateTimeOffset</td>
    <td>Long</td>
  </tr>
  <tr>
    <td>Nullable&lt;T&gt;</td>
    <td>Result of type inference on T</td>
  </tr>
  <tr>
    <td>IEnumerable&lt;T&gt;</td>
    <td>Array&lt;Result of type inference on T&gt;</td>
  </tr>
  <tr>
    <td>If property type does not match any above types</td>
    <td>Complex&lt;json&gt;</td>
  </tr>
</tbody>
</table>

## Refering to objects representing data
You can refer objects representing your query data in two way:
- by its properties, resulting in library mapping them to Druid columns
- by it as a whole, resulting in library mapping the whole object to a column.

This means the following queries will give you equivalent results.

```cs
record Aggregations(int AddedSum);
var first = new Query<Edit>
    .TimeSeries
    .WithNoVirtualColumns
    .WithAggregations<Aggregations>()
    .Aggregations(type => new(
        type.Sum(edit => edit.Added)));
var second = new Query<Edit>
    .TimeSeries
    .WithNoVirtualColumns
    .WithAggregations<int>()
    .Aggregations(type => type.Sum(edit => edit.Added));
```

## Ternary expressions and type.None
`Expression<Delegate>` query paramers in your queries may contain [ternary expressions](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/operators/conditional-operator). Upon query execution (or mapping of a query to json) any ternary expressions will have their conditions evaluated and then will get replated with the result expressions matching the condition values.

```cs
var value = 1;
Func<bool> valueGreaterThanZero = () => value > 0;
var okTernaryExpressions = new Query<IotMeasurement>
    .TimeSeries
    .WithNoVirtualColumns
    .WithAggregations<AggregationsFromTernary>
    .WithPostAggregations<int>()
    .Aggregations(type => new(
        value > 0 ? type.Max(data => data.Value) : type.Min(data => data.Value),
        type.Last(data => valueGreaterThanZero() ? data.Timestamp : data.ProcessedTimestamp),
        value > 0 ? 
            (value == 1 ?
                type.First(data => data.Value) :
                type.Last(data => data.Value)) :
            type.Min(data => data.Value),
        type.Last(data => valueGreaterThanZero() ? 
            (valueGreaterThanZero() ? data.Timestamp : data.ProcessedTimestamp) :
            data.ProcessedTimestamp)))
    .PostAggregations(type => valueGreaterThanZero() ? type.Constant(1) : type.Constant(0));
```

Objects representing all possible operations on input data contain method `None`, calling which is equivalent to calling no method at all.

```cs
bool includeCount = true;
var conditionalCount = new Query<Edit>
    .TimeSeries
    .WithNoVirtualColumns
    .WithAggregations<int>()
    .Aggregations(type => includeCount ? type.Count() : type.None<int>());
```

## Druid expressions
The library accepts [Druid expressions](https://druid.apache.org/docs/latest/querying/math-expr) in form of a delegate where given object representing data available at that point in a query you are supposed to return an [interpolated string using $](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/tokens/interpolated) where each string's parameter is either:

- a property of object representing data, which will get mapped to appropriate column
- a constant, which will get converted to a string.

Passing any other parameters will result in an `InvalidOperationException` being thrown upon execution of the query.

```cs
var okExpressions = new Query<Edit>
    .TimeSeries
    .WithVirtualColumns<int>()
    .VirtualColumns(type => type.Expression<int>(edit => "1"))
    .VirtualColumns(type => type.Expression<int>(edit => $"{edit.Added} * 2"))
    .VirtualColumns(type => type.Expression<int>(edit =>
        $"{edit.Added} * 2" +
        $"- {edit.Deleted}"));
```

## Query result deserialization
The library serializes queries and deserializes query results using System.Text.Json. The serializer has been altered in following ways:
- applied `System.Text.Json.JsonSerializerDefaults.Web`
- `DateTime` and `DateTimeOffset` can additionally be deserialized from unix timestamps
- `bool` can additionally be deserialized from "true", "false', "True" and "False" string literals in quotes
- `bool` can additionally be deserialized from numbers, where `1` will get deserialized to `true`, other numbers - to `false`
- applied various json converters for types defined in the library.

Get the default altered serializer options by calling `Apache.Druid.Querying.Json.DefaultSerializerOptions.Create()`.

Wherever possible, the query results have been "flattened" so they are streamed to consumers as soon as possible.

## Truncated query result handling
Apache Druid returns query results in form of http/1.1 responses with content-encoding: chunked. Because of that there's a chance of query results getting truncated, resulting in consumers getting only part of them. `Apache.Druid.Querying.DataSource<TSource>.ExecuteQuery` accepts parameter `onTruncatedResultsQueryForRemaining`, which if set to `true` (the default) will result in the library requesting the rest of the results in most of such cases, specifically:
1. Tcp connections closing or resetting before having streamed whole the response content.
2. Http responses completing successfully, but containing incomplete json.

In practice, the only unhandled case is when results are truncated due to [Apache Druid timeout feature](https://druid.apache.org/docs/latest/querying/query-context/#general-parameters). The way it works is when the timeout is reached, related http response completes successfully, with a complete json missing some of the results. [There is an (unfortunately stale) pull request changing the behaviour to follow case 1. from the previous paragraph](https://github.com/apache/druid/pull/13492). I consider this a bug in Druid itself. Until addressed by the Druid team, I recommend not to use Druid timeouts at all. Instead, if needed, apply timeouts through an http proxy or using cancellation tokens passed to `Apache.Druid.Querying.DataSource<TSource>.ExecuteQuery`.

Truncated result handling applies only in cases of truncated results, meaning http responses where at least response headers have successfully been read and so is not a retry policy. If needed, set up a retry policy yourself, using extensibility points provided by your chosen dependency injection library.
