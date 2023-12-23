namespace Apache.Druid.Querying
{
    public class Context
    {
        public long? Timeout { get; set; }
        public int? Priority { get; set; }
        public string? Lane { get; set; }
        public string? QueryId { get; set; }
        public string? BrokerService { get; set; }
        public bool? UseCache { get; set; }
        public bool? PopulateCache { get; set; }
        public bool? UseResultLevelCache { get; set; }
        public bool? PopulateResultLevelCache { get; set; }
        public bool? BySegment { get; set; }
        public bool? Finalize { get; set; }
        public long? MaxScatterGatherBytes { get; set; }
        public long? MaxQueuedBytes { get; set; }
        public long? MaxSubqueryRows { get; set; }
        public long? MaxSubqueryBytes { get; set; }
        public bool? SerializeDateTimeAsLong { get; set; }
        public bool? SerializeDateTimeAsLongInner { get; set; }
        public bool? EnableParallelMerge { get; set; }
        public int? ParallelMergeParallelism { get; set; }
        public int? ParallelMergeInitialYieldRows { get; set; }
        public int? ParallelMergeSmallBatchRows { get; set; }
        public bool? UseFilterCNF { get; set; }
        public bool? SecondaryPartitionPrunning { get; set; }
        public bool? EnableJoinLeftTableScanDirect { get; set; }
        public bool? Debug { get; set; }
        public bool? SetProcessingThreadNames { get; set; }
        public int? MaxNumericInFilters { get; set; }
        public int? InSubQueryThreshold { get; set; }

        public class WithVectorization : Context
        {
            public VectorizeMode? Vectorize { get; set; }
            public int? VectorSize { get; set; }
            public VectorizeMode? VectorizeVirtualColumns { get; set; }

            public enum VectorizeMode
            {
                True,
                False,
                Force
            }
        }
    }
}
