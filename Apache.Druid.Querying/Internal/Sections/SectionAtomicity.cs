using Apache.Druid.Querying.Internal.QuerySectionFactory;
using System;
using System.Collections.Generic;

namespace Apache.Druid.Querying.Internal.Sections
{
    internal sealed record SectionAtomicity(bool Atomic, string ColumnNameIfAtomic)
    {
        public interface IProvider
        {
            SectionAtomicity Get<TSection>();

            public sealed class Builder : IProvider
            {
                private readonly Dictionary<Type, SectionAtomicity> state = new();

                public SectionAtomicity Get<TSection>() => state[typeof(TSection)];

                public SectionAtomicity Add<TSection>(IReadOnlyList<ElementFactoryCall> calls, string columnNameIfAtomic)
                {
                    var atomic = calls.Count is 1 && calls[0].ResultMemberName is null;
                    var result = new SectionAtomicity(atomic, columnNameIfAtomic);
                    state.Add(typeof(TSection), result);
                    return result;
                }
            }
        }
    }
}
