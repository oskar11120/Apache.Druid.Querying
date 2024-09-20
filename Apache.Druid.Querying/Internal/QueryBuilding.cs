using System.Text.Json.Nodes;
using System.Text.Json;
using System;
using Apache.Druid.Querying.Internal.Sections;
using System.Linq.Expressions;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Apache.Druid.Querying.Internal;

public sealed record QueryToJsonMappingContext(
    JsonSerializerOptions QuerySerializerOptions,
    JsonSerializerOptions DataSerializerOptions,
    PropertyColumnNameMapping.IProvider ColumnNames);
public delegate JsonNode? QuerySectionToJson<TSection>(TSection Section, QueryToJsonMappingContext context);
public sealed record QuerySectionState<TSection>(string Key, TSection Section, QuerySectionToJson<TSection>? SectionToJson = null);

public delegate JsonNode? GetQuerySectionJson(QueryToJsonMappingContext context);
public sealed record QuerySectionFactoryState<TMarker>(string Key, GetQuerySectionJson GetJson);
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

    public interface Section<TSection> : JsonApplicableState<QuerySectionState<TSection>>
    {
        protected internal TSection Require() => State is not null ?
            State.Section :
            throw new InvalidOperationException($"Mssing required query section: {typeof(TSection)}.")
            {
                Data = { ["query"] = this }
            };

        void JsonApplicableState<QuerySectionState<TSection>>.ApplyOnJson(
            JsonObject json, QueryToJsonMappingContext context)
        {
            if (State is null)
                return;

            var (key, section, mapToJson) = State;
            var sectionJson = mapToJson is null ?
                JsonSerializer.SerializeToNode(section, context.QuerySerializerOptions) :
                mapToJson(section, context);
            if (sectionJson is not null)
                json[key.ToCamelCase()] = sectionJson;
        }

        internal void SetState(string key, TSection section, QuerySectionToJson<TSection>? sectionToJson = null)
            => State = new(key, section, sectionToJson);

        internal void SetState(string key, TSection section, Func<TSection, JsonNode?> sectionToJson)
            => State = new(key, section, sectionToJson is null ? null : (section, _) => sectionToJson(section));
    }

    private static void AddStateToJson(
        string key, GetQuerySectionJson getJson, JsonObject json, QueryToJsonMappingContext context)
    {
        var sectionJson = getJson(context);
        if (sectionJson is not null)
            json[key.ToCamelCase()] = sectionJson;
    }

    public interface SectionFactory<TSection> : JsonApplicableState<QuerySectionFactoryState<TSection>>
    {
        void JsonApplicableState<QuerySectionFactoryState<TSection>>.ApplyOnJson(
            JsonObject json, QueryToJsonMappingContext context)
        {
            if (State is null)
                return;

            AddStateToJson(State.Key, State.GetJson, json, context);
        }

        internal void SetState(string key, GetQuerySectionJson getJson)
            => State = new(key, getJson);

        internal void SetState(string key, Func<QueryToJsonMappingContext, TSection> factory)
            => SetState(key, context => JsonSerializer.SerializeToNode(factory(context), context.QuerySerializerOptions));
    }

    public interface SectionAtomicity : State<Sections.SectionAtomicity.ImmutableBuilder>
    {
        internal Sections.SectionAtomicity.ImmutableBuilder SectionAtomicity { get => State ??= new(); }
    }

    public interface MutableSectionAtomicity : SectionAtomicity
    {
        protected internal new Sections.SectionAtomicity.ImmutableBuilder SectionAtomicity { get => State ??= new(); set => State = value; }
    }

    public interface SectionFactoryExpressionStates : JsonApplicableState<Dictionary<string, GetQuerySectionJson>>
    {
        private protected void SetState(string key, GetQuerySectionJson getJson)
        {
            State ??= new();
            State.Remove(key);
            State.Add(key, getJson);
        }

        void JsonApplicableState<Dictionary<string, GetQuerySectionJson>>.ApplyOnJson(
            JsonObject json, QueryToJsonMappingContext context)
        {
            if (State is null)
                return;

            foreach (var (key, state) in State)
                AddStateToJson(key, state, json, context);
        }

        void State<Dictionary<string, GetQuerySectionJson>>.CopyFrom(
            State<Dictionary<string, GetQuerySectionJson>> other)
            => State = other.State is null ? new() : new Dictionary<string, GetQuerySectionJson>(other.State);
    }

    public interface SectionFactoryExpression<out TArguments, TSection, out TSectionKind> :
        MutableSectionAtomicity,
        SectionFactoryExpressionStates
        where TSectionKind : SectionKind
    {
        internal void SetState<TElementFactory>(
            string key,
            Expression<QuerySectionFactory<TElementFactory, TSection>> factory,
            SectionFactoryJsonMapper.Options? mapperOptions = null)
        {
            var calls = SectionFactoryParser
                .Execute(
                    factory,
                    typeof(TElementFactory),
                    typeof(TArguments))
                .ToList();
            SectionAtomicity = SectionAtomicity.Add<TSection>(calls, key, out var atomicity);
            SetState(key, context => SectionFactoryJsonMapper.Map<TArguments>(
                calls, atomicity, context, mapperOptions ?? SectionFactoryJsonMapper.Options.Default));
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
