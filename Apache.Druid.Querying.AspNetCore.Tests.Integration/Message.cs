namespace Apache.Druid.Querying.AspNetCore.Tests.Integration;

internal record Message(
    Guid TenantId,
    [property: DataSourceColumn("variable")] string VariableName,
    Guid ObjectId,
    double Value,
    [property: DataSourceTimeColumn] DateTimeOffset Timestamp,
    DateTimeOffset ProcessedTimestmap);
