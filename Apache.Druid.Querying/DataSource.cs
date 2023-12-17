using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Apache.Druid.Querying
{
    public interface IQueryWithResult<TResult> : IQuery
    {
    }

    public interface IQueryExecutor
    {
        IAsyncEnumerable<TResult> Execute<TResult>(Dictionary<string, object?> query, CancellationToken token);
    }

    public class DataSource<TSource>
    {
        protected readonly string Id;
        protected readonly IQueryExecutor Executor;

        public DataSource(string id, IQueryExecutor executor)
        {
            Id = id;
            Executor = executor;
        }

        public virtual IAsyncEnumerable<TResult> ExecuteQuery<TResult>(IQueryWithResult<TResult> query, CancellationToken token = default)
        {
            var asDictionary = query
                .GetState()
                .ToDictionary(pair => pair.Key, pair => pair.Value.Value);
            asDictionary.Add(nameof(DataSource<TSource>), Id);
            return Executor.Execute<TResult>(asDictionary, token);
        }
    }
}
