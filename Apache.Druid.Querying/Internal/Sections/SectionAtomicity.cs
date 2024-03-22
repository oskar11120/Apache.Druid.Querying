using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Druid.Querying.Internal.Sections
{
    public sealed record SectionAtomicity(bool Atomic, string ColumnNameIfAtomic, byte[] ColumnNameIfAtomicUtf8)
    {
        public SectionAtomicity WithColumnNameIfAtomic(string columnName)
            => new(Atomic, columnName, Encoding.UTF8.GetBytes(columnName));

        public interface IProvider
        {
            SectionAtomicity? TryGet<TSection>();

            public sealed class Builder : IProvider
            {
                private static readonly Dictionary<Type, SectionAtomicity> empty = new();
                private readonly Dictionary<Type, SectionAtomicity> state;

                public Builder(Dictionary<Type, SectionAtomicity>? state = null)
                    => this.state = state ?? new();

                public SectionAtomicity? TryGet<TSection>() => state.GetValueOrDefault(typeof(TSection));

                internal SectionAtomicity Add<TSection>(IReadOnlyList<ElementFactoryCall> calls, string columnNameIfAtomic)
                {
                    var atomic = calls.Count is 1 && calls[0].ResultMemberName is null;
                    var result = new SectionAtomicity(atomic, columnNameIfAtomic, Encoding.UTF8.GetBytes(columnNameIfAtomic));
                    state.Add(typeof(TSection), result);
                    return result;
                }

                public Builder Updated(Func<SectionAtomicity, SectionAtomicity> update)
                    => new(state
                    .ToDictionary(pair => pair.Key, pair => update(pair.Value)));

                public static Builder Combined(Builder? first, Builder? second, Builder? third = null)
                {
                    var states = new
                    {
                        First = first?.state ?? empty,
                        Second = second?.state ?? empty,
                        Third = third?.state ?? empty,
                    };

                    var length = states.First.Count + states.Second.Count + states.Third.Count;
                    if (length is 0)
                        return new();

                    var result = new Dictionary<Type, SectionAtomicity>();
                    var all = states.First.Concat(states.Second).Concat(states.Third);
                    foreach (var (type, atomicity) in all)
                        result.TryAdd(type, atomicity);
                    return new(result);
                }
            }
        }
    }
}
