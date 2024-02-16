namespace Apache.Druid.Querying.Tests.Integration;

[DataSourceColumnNamingConvention(DataSourceColumnNamingConventionType.CamelCase)]
internal record VariableMessage(
    Guid TenantId,
    [property: DataSourceColumn("variable")] string VariableName,
    Guid ObjectId,
    double Value,
    [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
    DateTimeOffset ProcessedTimestamp);

[DataSourceColumnNamingConvention(DataSourceColumnNamingConventionType.CamelCase)]
internal record EventMessage(
    Guid TenantId,
    [property: DataSourceColumn("event")] string EventName,
    Guid ObjectId,
    double Value,
    [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
    DateTimeOffset ProcessedTimestamp);
