namespace Apache.Druid.Querying.AspNetCore.Tests.Integration;

[DataSourceColumnNamingConvention(DataSourceColumnNamingConventionType.CamelCase)]
internal record Message(
    Guid TenantId,
    [property: DataSourceColumnAttribute("variable")] string VariableName,
    Guid ObjectId,
    double Value,
    [property: DataSourceTimeColumnAttribute] DateTimeOffset Timestamp,
    DateTimeOffset ProcessedTimestamp);
