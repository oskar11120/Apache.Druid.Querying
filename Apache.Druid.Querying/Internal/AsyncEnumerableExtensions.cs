using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Apache.Druid.Querying.Internal;

internal static class AsyncEnumerableExtensions
{
    public static async IAsyncEnumerable<TItem> Catch<TItem, TException>(
        this IAsyncEnumerable<TItem> source,
        Action<TException> handler,
        [EnumeratorCancellation] CancellationToken token)
        where TException : Exception
    {
        var enumerator = source.GetAsyncEnumerator(token);
        while (true)
        {
            TItem item;
            try
            {
                if (!await enumerator.MoveNextAsync())
                    break;
                item = enumerator.Current;
            }
            catch (TException ex)
            {
                handler(ex);
                break;
            }

            yield return item;
        }
    }
}
