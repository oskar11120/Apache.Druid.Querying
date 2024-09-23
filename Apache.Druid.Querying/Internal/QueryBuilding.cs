using System.Text.Json.Nodes;
using System.Text.Json;
using System;
using Apache.Druid.Querying.Internal.Sections;
using System.Linq.Expressions;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Immutable;
using static Apache.Druid.Querying.Internal.IQueryWithInternal;

namespace Apache.Druid.Querying.Internal;

public sealed record QueryToJsonMappingContext(
    JsonSerializerOptions QuerySerializerOptions,
    JsonSerializerOptions DataSerializerOptions,
    PropertyColumnNameMapping.IProvider ColumnNames);

public delegate PropertyColumnNameMapping.ImmutableBuilder ApplyPropertyColumnNameMappingChanges(
    PropertyColumnNameMapping.ImmutableBuilder existing);

public static class IQueryWithInternal
{
    public record SectionKind
    {
        public sealed record VirtualColumns : SectionKind;
        public sealed record Aggregations : SectionKind;
        public sealed record PostAggregations : SectionKind;
        public sealed record Dimension : SectionKind;
        public sealed record Dimensions : SectionKind;
    }

    public interface State<TState> : IQueryWith.State
    {
        protected internal TState? State { get; set; }
        internal void CopyFrom(State<TState> other) => State = other.State;
    }

    public interface JsonApplicableState<TState> : State<TState>
    {
        internal void ApplyOnJson(JsonObject json, QueryToJsonMappingContext context);
    }

    public interface Section<TSection> : JsonApplicableState<TSection>
    {
        private protected abstract string Key { get; }

        private protected virtual JsonNode? ToJson(TSection section, QueryToJsonMappingContext context)
            => JsonSerializer.SerializeToNode(section, context.QuerySerializerOptions);

        protected internal TSection Require() => State is not null ?
            State :
            throw new InvalidOperationException($"Mssing required query section: {typeof(TSection)}.")
            {
                Data = { ["query"] = this }
            };

        void JsonApplicableState<TSection>.ApplyOnJson(JsonObject json, QueryToJsonMappingContext context)
        {
            if (State is null)
                return;

            var sectionJson = ToJson(State, context);
            if (sectionJson is not null)
                json[Key.ToCamelCase()] = sectionJson;
        }
    }

    public delegate TSection CreateSection<TSection>(QueryToJsonMappingContext context);
    public interface SectionFactory<TSection> : Section<CreateSection<TSection>>
    {
        JsonNode? Section<CreateSection<TSection>>.ToJson(
            CreateSection<TSection> factory, QueryToJsonMappingContext context)
        {
            var section = factory(context);
            return JsonSerializer.SerializeToNode(section, context.QuerySerializerOptions);
        }
    }

    public interface StateMappedToSection<TState, TSection> : Section<TState>
    {
        private protected virtual TSection ToSection(TState state, QueryToJsonMappingContext context) => ToSection(state);
        private protected virtual TSection ToSection(TState state) => throw new NotSupportedException();

        JsonNode? Section<TState>.ToJson(TState state, QueryToJsonMappingContext context)
        {
            var section = ToSection(state, context);
            return JsonSerializer.SerializeToNode(section, context.QuerySerializerOptions);
        }
    }

    public interface SectionAtomicity : State<Sections.SectionAtomicity.ImmutableBuilder>
    {
        internal Sections.SectionAtomicity.ImmutableBuilder SectionAtomicity { get => State ??= new(); }
    }

    public interface MutableSectionAtomicity : SectionAtomicity
    {
        protected internal new Sections.SectionAtomicity.ImmutableBuilder SectionAtomicity { get => State ??= new(); set => State = value; }
    }

    public sealed record SectionFactoryExpressionState<TSectionKind>(
        IReadOnlyList<ElementFactoryCall> Calls, 
        Sections.SectionAtomicity Atomicity,
        SectionFactoryJsonMapper.Options? MapperOptions);
    public interface SectionFactoryExpression<out TArguments, TSection, TSectionKind> :
        MutableSectionAtomicity,
        Section<SectionFactoryExpressionState<TSectionKind>>
        where TSectionKind : SectionKind
    {
        private Section<SectionFactoryExpressionState<TSectionKind>> AsExpression => this;

        JsonNode? Section<SectionFactoryExpressionState<TSectionKind>>.ToJson(SectionFactoryExpressionState<TSectionKind> state, QueryToJsonMappingContext context)
            => SectionFactoryJsonMapper.Map<TArguments>(
                state.Calls, state.Atomicity, context, state.MapperOptions ?? SectionFactoryJsonMapper.Options.Default);

        internal void SetState<TElementFactory>(
            Expression<QuerySectionFactory<TElementFactory, TSection>> factory,
            SectionFactoryJsonMapper.Options? mapperOptions = null)
        {
            var calls = SectionFactoryParser
                .Execute(
                    factory,
                    typeof(TElementFactory),
                    typeof(TArguments))
                .ToList();
            SectionAtomicity = SectionAtomicity.Add<TSection>(calls, Key, out var atomicity);
            AsExpression.State = new SectionFactoryExpressionState<TSectionKind>(calls, atomicity, mapperOptions);
        }
    }

    public interface PropertyColumnNameMappingChanges : State<ImmutableDictionary<Type, ApplyPropertyColumnNameMappingChanges>>
    {
        internal sealed ApplyPropertyColumnNameMappingChanges ApplyPropertyColumnNameMappingChanges =>
            existing => State is null ?
                existing :
                State
                    .Values
                    .Aggregate(existing, (all, applyNew) => applyNew(all));

        protected internal sealed void Set<TModel>(Func<PropertyColumnNameMapping.IProvider, ImmutableArray<PropertyColumnNameMapping>> factory)
        {
            State ??= ImmutableDictionary<Type, ApplyPropertyColumnNameMappingChanges>.Empty;
            State = State.SetItem(typeof(TModel), mappings => mappings.Add<TModel>(factory(mappings)));
        }
    }
}
