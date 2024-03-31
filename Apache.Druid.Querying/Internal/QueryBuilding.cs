using static Apache.Druid.Querying.IQueryWith;
using System.Text.Json.Nodes;
using System.Text.Json;
using System;
using Apache.Druid.Querying.Internal.Sections;
using System.Linq.Expressions;
using System.Linq;
using System.Collections.Generic;

namespace Apache.Druid.Querying.Internal;

public delegate JsonNode? QuerySectionToJson<TSection>(TSection Section, JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames);
public sealed record QuerySectionState<TSection>(string Key, TSection Section, QuerySectionToJson<TSection>? SectionToJson = null);

public delegate JsonNode? GetQuerySectionJson(JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames);
public sealed record QuerySectionFactoryState<TMarker>(string Key, GetQuerySectionJson GetJson);

public static class IQueryWithInternal
{
    public static class Marker
    {
        public sealed record VirtualColumns;
        public sealed record Aggregations;
        public sealed record PostAggregations;
        public sealed record Dimension;
        public sealed record Dimensions;
    }

    public interface State<TState> : State
    {
        protected internal TState? State { get; set; }
        internal void AddToJson(JsonObject json, JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames);
        internal void CopyFrom(State<TState> other) => State = other.State;
    }

    internal static IEnumerable<Type> GetStateInterfaces(this IQueryWith.State query) => query
        .GetType()
        .GetInterfaces()
        .Where(@interface => @interface.IsGenericType && @interface.GetGenericTypeDefinition() == typeof(State<>));

    public interface Section<TSection> : State<QuerySectionState<TSection>>
    {
        protected internal TSection Require() => State is not null ?
            State.Section :
            throw new InvalidOperationException($"Mssing required query section: {nameof(Intervals)}.")
            {
                Data = { ["query"] = this }
            };

        void State<QuerySectionState<TSection>>.AddToJson(
            JsonObject json, JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames)
        {
            if (State is null)
                return;

            var (key, section, mapToJson) = State;
            var sectionJson = mapToJson is null ?
                JsonSerializer.SerializeToNode(section, serializerOptions) :
                mapToJson(section, serializerOptions, columnNames);
            if (sectionJson is not null)
                json[key.ToCamelCase()] = sectionJson;
        }

        internal void SetState(string key, TSection section)
            => State = new(key, section, null);

        internal void SetState(string key, TSection section, Func<TSection, JsonNode?> sectionToJson)
            => State = new(key, section, sectionToJson is null ? null : (section, _, _) => sectionToJson(section));

        internal void SetState(string key, TSection section, Func<TSection, JsonSerializerOptions, JsonNode?> sectionToJson)
            => State = new(key, section, sectionToJson is null ? null : (section, options, _) => sectionToJson(section, options));
    }

    private static void AddStateToJson(
        string key, GetQuerySectionJson getJson, JsonObject json, JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames)
    {
        var sectionJson = getJson(serializerOptions, columnNames);
        if (sectionJson is not null)
            json[key.ToCamelCase()] = sectionJson;
    }

    public interface SectionFactory<TSection> : State<QuerySectionFactoryState<TSection>>
    {
        void State<QuerySectionFactoryState<TSection>>.AddToJson(
            JsonObject json, JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames)
        {
            if (State is null)
                return;

            AddStateToJson(State.Key, State.GetJson, json, serializerOptions, columnNames);
        }

        internal void SetState(string key, GetQuerySectionJson getJson)
            => State = new(key, getJson);

        internal void SetState(string key, Func<IColumnNameMappingProvider, TSection> factory)
            => SetState(key, (serializeOptions, columnNames) => JsonSerializer.SerializeToNode(factory(columnNames), serializeOptions));
    }

    public interface SectionFactoryExpression_Atomicity : State<SectionAtomicity.IProvider.ImmutableBuilder>
    {
        internal SectionAtomicity.IProvider.ImmutableBuilder SectionAtomicity { get => State ??= new(); set => State = value; }

        void State<SectionAtomicity.IProvider.ImmutableBuilder>.AddToJson(
            JsonObject json, JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames)
        {
        }
    }

    public interface SectionFactoryExpression_States : State<Dictionary<string, GetQuerySectionJson>>
    {
        private protected void SetState(string key, GetQuerySectionJson getJson)
        {
            State ??= new();
            State.Remove(key);
            State.Add(key, getJson);
        }

        void State<Dictionary<string, GetQuerySectionJson>>.AddToJson(
            JsonObject json, JsonSerializerOptions serializerOptions, IColumnNameMappingProvider columnNames)
        {
            if (State is null)
                return;

            foreach (var (key, state) in State)
                AddStateToJson(key, state, json, serializerOptions, columnNames);
        }

        void State<Dictionary<string, GetQuerySectionJson>>.CopyFrom(
            State<Dictionary<string, GetQuerySectionJson>> other)
            => State = other.State is null ? new() : new Dictionary<string, GetQuerySectionJson>(other.State);
    }

    public interface SectionFactoryExpression<TArguments, TSection, TMarker> :
        SectionFactoryExpression_Atomicity,
        SectionFactoryExpression_States
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
                    typeof(TArguments),
                    typeof(TSection))
                .ToList();
            SectionAtomicity = SectionAtomicity.Add<TSection>(calls, key, out var atomicity);
            SetState(key, (serializerOptions, columnNames) => SectionFactoryJsonMapper.Map<TArguments>(
                calls, atomicity, serializerOptions, columnNames, mapperOptions ?? SectionFactoryJsonMapper.Options.Default));
        }
    }
}
