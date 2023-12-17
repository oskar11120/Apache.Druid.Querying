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
        private readonly string id;
        private readonly IQueryExecutor executor;

        public DataSource(string id, IQueryExecutor executor)
        {
            this.id = id;
            this.executor = executor;
        }

        public virtual IAsyncEnumerable<TResult> ExecuteQuery<TResult>(IQueryWithResult<TResult> query, CancellationToken token = default)
        {
            var asDictionary = query
                .GetState()
                .ToDictionary(pair => pair.Key, pair => pair.Value.Value);
            asDictionary.Add(nameof(DataSource<TSource>), id);
            return executor.Execute<TResult>(asDictionary, token);
        }
    }
}
