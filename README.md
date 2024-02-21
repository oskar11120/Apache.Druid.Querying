# [Apache Druid](http://druid.io/) client library/micro-orm for dotnet 6+ inspired by [EF Core](https://learn.microsoft.com/pl-pl/ef/core/).

https://www.nuget.org/packages/Apache.Druid.Querying

## Setup
To make your Druid data sources available for querying create a class inheriting from `Apache.Druid.Querying.DataSourceProvider`. The class represents collection of data sources available for querying similarity to how `EfCore`'s `DbContext` represents collection of database tables. The class contains methods `Table`, `Lookup` and `Inline` which you can use to create instances of `Apache.Druid.Querying.DataSource` (similar to `EfCore`'s `DbSet`) which in turn turn can be used of querying. The instances are thread safe and so can be used for executing multiple queries at the same time. Some of the `DataSource` creating methods require parameter `id` which corresponds to id of related `Druid` data source.

The method `Table` additionally requires generic parameter `TSource` depicting a row of your table data, similarily to how `EfCore`'s `Entities` depict database rows. The type's public properties correspond to the data source columns.

By default `TSource` property names map 1-to-1 into `Druid` data source column names. This can be overriden in two ways:
- By decorating `TSource` with `Apache.Druid.Querying.DataSourceNamingConvention` attribute. The convention will applied to all `TSource`'s property names.
- By decorating `TSource`'s properties with `Apache.Druid.Querying.DataSourceColumn` attribute. The string parameter passed to the attrubute will become the data source column name. As most `Druid` data source contain column `__time` for convenience there exists attribute `Apache.Druid.Querying.DataSourceTimeColumn` equivalent to `Apache.Druid.Querying.DataSourceColumn("__time")`.

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
            // Corresponde to Druid's example wikipedia edits data.
            Edits = Table<Edit>("wikipedia");
        }

        public DataSource<Edit> Edits { get; }
    }
```

Then connect up your data source provider to a depency injection framework of your choice:
- [Microsoft.Extensions.DependencyInjection](Apache.Druid.Querying.Microsoft.Extensions.DependencyInjection/README.md)

## Querying
Choose query type and models representing query's data using nested types of `Apache.Druid.Querying.Query<TSource>`. Create a query by instantiating chosen nested type. Set query data by calling the instance methods. The methods often accept `Expression<Delegate>`, using which given an object representing input data available at that point in a query and an object representing all possible operations on that input data, you create an object representing results of your chosen operation. To get an idea on what's possible it's best look into project's tests. The queries have been designed so as much information as possible is available complie time. Wherever possible, the query results have been "flattened" so they are streams to their consumer as soon as possible.

Currently available query types:
- TimeSeries
- TopN
- GroupBy
- Scan (currently missing option to specifiy a subset columns)

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
