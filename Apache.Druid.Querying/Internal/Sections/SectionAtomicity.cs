using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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

            public sealed class ImmutableBuilder : IProvider
            {
                private static readonly ImmutableDictionary<Type, SectionAtomicity> empty = ImmutableDictionary<Type, SectionAtomicity>.Empty;
                private readonly ImmutableDictionary<Type, SectionAtomicity> state;

                public ImmutableBuilder(ImmutableDictionary<Type, SectionAtomicity>? state = null)
                    => this.state = state ?? empty;

                public SectionAtomicity? TryGet<TSection>() => state.GetValueOrDefault(typeof(TSection));

                internal ImmutableBuilder Add<TSection>(IReadOnlyList<ElementFactoryCall> calls, string columnNameIfAtomic, out SectionAtomicity added)
                {
                    var atomic = calls.Count is 1 && calls[0].ResultMemberName is null;
                    added = new(atomic, columnNameIfAtomic, Encoding.UTF8.GetBytes(columnNameIfAtomic));
                    return new(state.Add(typeof(TSection), added));
                }

                public ImmutableBuilder Update(Func<SectionAtomicity, SectionAtomicity> update)
                    => new(state
                    .ToImmutableDictionary(pair => pair.Key, pair => update(pair.Value)));

                public static ImmutableBuilder Combine(ImmutableBuilder? first, ImmutableBuilder? second, ImmutableBuilder? third = null)
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

                    var builder = ImmutableDictionary.CreateBuilder<Type, SectionAtomicity>();
                    var all = states.First.Concat(states.Second).Concat(states.Third);
                    foreach (var (type, atomicity) in all)
                        builder.TryAdd(type, atomicity);
                    return new(builder.ToImmutableDictionary());
                }
            }
        }
    }
}
