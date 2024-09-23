using Apache.Druid.Querying.Internal;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Apache.Druid.Querying
{
    public sealed class None
    {
        public static readonly None Singleton = new();
    }

    public partial record QueryDataKind
    {
        public sealed record ScanResultValue : QueryDataKind;
        public sealed record WithTimestamp : QueryDataKind;
        public sealed record VirtualColumns : QueryDataKind;
        public sealed record Dimensions : QueryDataKind;
        public sealed record Aggregations : QueryDataKind;
        public sealed record PostAggregations : QueryDataKind;
    }

    public sealed record Source_VirtualColumns<TSource, TVirtualColumns>(TSource Source, TVirtualColumns VirtualColumns) :
        IQueryData<TSource, QueryDataKind.Source>,
        IQueryData<TVirtualColumns, QueryDataKind.VirtualColumns>
    {
        TSource IQueryData<TSource, QueryDataKind.Source>.Value => Source;
        TVirtualColumns IQueryData<TVirtualColumns, QueryDataKind.VirtualColumns>.Value => VirtualColumns;
    }

    public sealed record ScanResult<TValue>(string? SegmentId, TValue Value)
        : IQueryData<TValue, QueryDataKind.ScanResultValue>
    {
    }

    public sealed record WithTimestamp<TValue>(DateTimeOffset Timestamp, TValue Value)
        : IQueryData<TValue, QueryDataKind.WithTimestamp>
    {
        private static readonly byte[] timeColumnUtf8Bytes = Encoding.UTF8.GetBytes("__time");

        internal static readonly QueryResultElement.Deserializer<WithTimestamp<TValue>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
                => new(
                    context.DeserializeProperty<DateTimeOffset>(timeColumnUtf8Bytes),
                    context.Deserialize<TValue>());
    }

    public sealed record Dimension_Aggregations<TDimension, TAggregations>(TDimension Dimension, TAggregations Aggregations) :
        IQueryData<TDimension, QueryDataKind.Dimensions>,
        IQueryData<TAggregations, QueryDataKind.Aggregations>
    {
        internal static readonly QueryResultElement.Deserializer<Dimension_Aggregations<TDimension, TAggregations>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TDimension>(),
                    context.Deserialize<TAggregations>());
        TDimension IQueryData<TDimension, QueryDataKind.Dimensions>.Value => Dimension;
        TAggregations IQueryData<TAggregations, QueryDataKind.Aggregations>.Value => Aggregations;
    }

    public sealed record Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>(
        TDimension Dimension, TAggregations Aggregations, TPostAggregations PostAggregations) :
        IQueryData<TDimension, QueryDataKind.Dimensions>,
        IQueryData<TAggregations, QueryDataKind.Aggregations>,
        IQueryData<TPostAggregations, QueryDataKind.PostAggregations>
    {
        internal static readonly QueryResultElement.Deserializer<Dimension_Aggregations_PostAggregations<TDimension, TAggregations, TPostAggregations>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimension>(),
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        TDimension IQueryData<TDimension, QueryDataKind.Dimensions>.Value => Dimension;
        TAggregations IQueryData<TAggregations, QueryDataKind.Aggregations>.Value => Aggregations;
        TPostAggregations IQueryData<TPostAggregations, QueryDataKind.PostAggregations>.Value => PostAggregations;
    }

    public sealed record Aggregations_PostAggregations<TAggregations, TPostAggregations>(TAggregations Aggregations, TPostAggregations PostAggregations) :
        IQueryData<TAggregations, QueryDataKind.Aggregations>,
        IQueryData<TPostAggregations, QueryDataKind.PostAggregations>
    {
        internal static readonly QueryResultElement.Deserializer<Aggregations_PostAggregations<TAggregations, TPostAggregations>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
                => new(
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        TAggregations IQueryData<TAggregations, QueryDataKind.Aggregations>.Value => Aggregations;
        TPostAggregations IQueryData<TPostAggregations, QueryDataKind.PostAggregations>.Value => PostAggregations;
    }

    public sealed record Dimensions_Aggregations<TDimensions, TAggregations>(TDimensions Dimensions, TAggregations Aggregations) :
        IQueryData<TDimensions, QueryDataKind.Dimensions>,
        IQueryData<TAggregations, QueryDataKind.Aggregations>
    {
        internal static readonly QueryResultElement.Deserializer<Dimensions_Aggregations<TDimensions, TAggregations>> Deserializer =
            (in QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimensions>(),
                    context.Deserialize<TAggregations>());
        TDimensions IQueryData<TDimensions, QueryDataKind.Dimensions>.Value => Dimensions;
        TAggregations IQueryData<TAggregations, QueryDataKind.Aggregations>.Value => Aggregations;
    }

    public sealed record Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>(
        TDimensions Dimensions, TAggregations Aggregations, TPostAggregations PostAggregations) :
        IQueryData<TDimensions, QueryDataKind.Dimensions>,
        IQueryData<TAggregations, QueryDataKind.Aggregations>,
        IQueryData<TPostAggregations, QueryDataKind.PostAggregations>
    {
        internal static readonly QueryResultElement.Deserializer<Dimensions_Aggregations_PostAggregations<TDimensions, TAggregations, TPostAggregations>> Deserializer =
          (in QueryResultElement.DeserializerContext context)
               => new(
                    context.Deserialize<TDimensions>(),
                    context.Deserialize<TAggregations>(),
                    context.Deserialize<TPostAggregations>());
        TDimensions IQueryData<TDimensions, QueryDataKind.Dimensions>.Value => Dimensions;
        TAggregations IQueryData<TAggregations, QueryDataKind.Aggregations>.Value => Aggregations;
        TPostAggregations IQueryData<TPostAggregations, QueryDataKind.PostAggregations>.Value => PostAggregations;
    }

    public sealed record DataSourceMetadata(DateTimeOffset MaxIngestedEventTime);

    public static class QueryContext
    {
        public class TimeSeries : Context.WithVectorization
        {
            public bool? SkipEmptyBuckets { get; set; }
        }

        public class TopN : Context
        {
            public int? MinTopNThreshold { get; set; }
        }

        public class GroupBy : Context.WithVectorization
        {
            public long? MaxOnDiskStorage { get; set; }
            public bool? GroupByIsSingleThreaded { get; set; }
            public bool? BufferGrouperInitialBuckets { get; set; }
            public double? BufferGrouperMaxLoadFactor { get; set; }
            public bool? ForceHashAggregation { get; set; }
            public int? IntermediateCombineDegree { get; set; }
            public int? NumParallelCombineThreads { get; set; }
            public bool? MergeThreadLocal { get; set; }
            public bool? SortByDimsFirst { get; set; }
            public bool? ForceLimitPushDown { get; set; }
            public bool? ApplyLimitPushDownToSegment { get; set; }
            public bool? GroupByEnableMultiValueUnnesting { get; set; }
        }

        public class Scan : Context
        {
            public int? MaxRowsQueuedForOrdering { get; set; }
            public int? MaxSegmentPartitionsOrderedInMemory { get; set; }
        }
    }

    public static class Query<TSource>
    {
        public class TimeSeries : QueryBase<TSource, TSource, TimeSeries>.TimeSeries
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TimeSeries,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.TimeSeries<TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            Source_VirtualColumns<TSource, TVirtualColumns>,
                            WithPostAggregations<TPostAggregations>>
                        .TimeSeries<TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                        IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, TSource, WithNoVirtualColumns>.TimeSeries
            {
                public class WithAggregations<TAggregations> : QueryBase<TSource, TSource, WithAggregations<TAggregations>>.TimeSeries<TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            TSource,
                            WithPostAggregations<TPostAggregations>>
                        .TimeSeries<TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class TopN<TDimension> : QueryBase<TSource, TSource, TopN<TDimension>>.TopN<TDimension>
            where TDimension : IEquatable<TDimension>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.TopN<TDimension>,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

                public class WithAggregations<TAggregations> :
                    QueryBase<
                        TSource,
                        Source_VirtualColumns<TSource, TVirtualColumns>,
                        WithAggregations<TAggregations>>
                    .TopN<TDimension, TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            Source_VirtualColumns<TSource, TVirtualColumns>,
                            WithPostAggregations<TPostAggregations>>
                        .TopN<TDimension, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                        IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, TSource, WithNoVirtualColumns>.TopN<TDimension>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<
                        TSource,
                        TSource,
                        WithAggregations<TAggregations>>
                    .TopN<TDimension, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            TSource,
                            WithPostAggregations<TPostAggregations>>
                        .TopN<TDimension, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class GroupBy<TDimensions> : QueryBase<TSource, TSource, GroupBy<TDimensions>>.GroupBy<TDimensions>
             where TDimensions : IEquatable<TDimensions>
        {
            public class WithVirtualColumns<TVirtualColumns> :
                QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithVirtualColumns<TVirtualColumns>>.GroupBy<TDimensions>,
                IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithVirtualColumns<TVirtualColumns>>
            {
                IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, Source_VirtualColumns<TSource, TVirtualColumns>, WithAggregations<TAggregations>>.GroupBy<TDimensions, TAggregations>,
                    IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithAggregations<TAggregations>>
                {
                    IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            Source_VirtualColumns<TSource, TVirtualColumns>,
                            WithPostAggregations<TPostAggregations>>
                        .GroupBy<TDimensions, TAggregations, TPostAggregations>,
                        IQueryWith.VirtualColumns<TSource, TVirtualColumns, WithPostAggregations<TPostAggregations>>
                    {
                        IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>? IQueryWithInternal.State<IQueryWithInternal.SectionFactoryExpressionState<IQueryWithInternal.SectionKind.VirtualColumns>>.State { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
                    }
                }
            }

            public class WithNoVirtualColumns : QueryBase<TSource, TSource, WithNoVirtualColumns>.GroupBy<TDimensions>
            {
                public class WithAggregations<TAggregations> :
                    QueryBase<TSource, TSource, WithAggregations<TAggregations>>.GroupBy<TDimensions, TAggregations>
                {
                    public class WithPostAggregations<TPostAggregations> :
                        QueryBase<
                            TSource,
                            TSource,
                            WithPostAggregations<TPostAggregations>>
                        .GroupBy<TDimensions, TAggregations, TPostAggregations>
                    {
                    }
                }
            }
        }

        public class Scan : QueryBase<TSource, TSource, Scan>.Scan<TSource>
        {
            public class WithColumns<TColumns> :
                QueryBase<TSource, TSource, WithColumns<TColumns>>.Scan<TColumns>,
                IQueryWithInternal.PropertyColumnNameMappingChanges,
                IQueryWithInternal.Section<WithColumns<TColumns>.PropertyMapping[]>
            {
                public sealed record PropertyMapping(string ColumnProperty, SelectedProperty SourceProperty);

                string IQueryWithInternal.Section<PropertyMapping[]>.Key => "columns";
                JsonNode? IQueryWithInternal.Section<PropertyMapping[]>.ToJson(PropertyMapping[] section, QueryToJsonMappingContext context) 
                {
                    var properties = Get(context.ColumnNames, section).Select(pair => pair.SourceMapping.ColumnName);
                    return JsonSerializer.SerializeToNode(properties, context.QuerySerializerOptions);
                }

                private static IEnumerable<(string ColumnProperty, PropertyColumnNameMapping SourceMapping)> Get(
                    PropertyColumnNameMapping.IProvider mappings,
                    PropertyMapping[] propertyMappings)
                {
                    var sourceColumnMappings = mappings.Get<TSource>();
                    foreach (var (columnProperty, sourceProperty) in propertyMappings)
                    {
                        var mapping = sourceColumnMappings
                            .SingleOrDefault(column => column.Property == sourceProperty.Name)
                            ?? throw new InvalidOperationException(
                                $"Property: {sourceProperty.SelectedFromType}.{sourceProperty.Name} does not " +
                                $"correspond to any {typeof(TSource)} data source column.");
                        yield return (columnProperty, mapping);
                    }
                }

                public WithColumns<TColumns> Columns(Expression<Func<TSource, TColumns>> mapSourceToColumns)
                {
                    try
                    {
                        var propertyMappings = mapSourceToColumns
                            .Body
                            .UnwrapUnary()
                            .GetPropertyAssignments(default(None), (_, error) => new InvalidOperationException(error))
                            .Select(pair => new PropertyMapping(pair.PropertyName, SelectedProperty.Get(pair.AssignedValue)))
                            .ToArray();
                        SelectedProperties.State = propertyMappings;


                        MappingChanges.Set<TColumns>(mappings =>
                        {
                            var columnMappings = Get(mappings, SelectedProperties.State)
                                .Select(pair => pair.SourceMapping with { Property = pair.ColumnProperty });
                            return columnMappings.ToImmutableArray();
                        });
                        return this;
                    }
                    catch (Exception inner)
                    {
                        throw new InvalidOperationException($"Invalid expression: {mapSourceToColumns}. {inner.Message}", inner);
                    }
                }

                private IQueryWithInternal.Section<PropertyMapping[]> SelectedProperties => this;
                private IQueryWithInternal.PropertyColumnNameMappingChanges MappingChanges => this;

                PropertyMapping[]? IQueryWithInternal.State<PropertyMapping[]>.State { get; set; }
            }
        }

        public class SegmentMetadata : QueryBase<TSource>.SegmentMetadata
        {
            public SegmentMetadata Merge(bool merge)
            {
                MergeSection.SetState(nameof(Merge), new(merge), state => state.Merge);
                return this;
            }

            public SegmentMetadata AnalysisTypes(IReadOnlyCollection<Querying.SegmentMetadata.AnalysisType> types)
            {
                AnalysisTypesSection.SetState(
                    nameof(AnalysisTypes),
                    types,
                    (types, context) => JsonSerializer.SerializeToNode(
                        types.Select(type => AnalysisTypeStrings[type]),
                        context.QuerySerializerOptions));
                return this;
            }

            public SegmentMetadata AnalysisTypes(params Querying.SegmentMetadata.AnalysisType[] types)
                => AnalysisTypes(types as IReadOnlyCollection<Querying.SegmentMetadata.AnalysisType>);

            public SegmentMetadata AggregatorMergeStrategy(Querying.SegmentMetadata.AggregatorMergeStrategy strategy)
            {
                MergeStrategySection.SetState(nameof(AggregatorMergeStrategy), strategy);
                return this;
            }
        }

        public class DataSourceMetadata :
            QueryBase,
            IQueryWith.Context<Context, DataSourceMetadata>,
            QueryResultDeserializer.ArrayOfObjectsWithTimestamp<Querying.DataSourceMetadata>,
            TruncatedQueryResultHandler<TSource>.TimeSeries<Querying.DataSourceMetadata>
        {
            QuerySectionState<Context>? IQueryWithInternal.State<QuerySectionState<Context>>.State { get; set; }
        }
    }
}
