package org.neo4j.cdc.client.pattern;

import org.jetbrains.annotations.NotNull;
import org.neo4j.cdc.client.selector.RelationshipSelector;
import org.neo4j.cdc.client.selector.Selector;

import java.util.Map;
import java.util.Set;

public class RelationshipPattern implements Pattern {
    private final String type;
    private final NodePattern start;
    private final NodePattern end;
    private final boolean bidirectional;
    private final Map<String, Object> keyFilters;
    private final Set<String> includeProperties;
    private final Set<String> excludeProperties;

    public RelationshipPattern(String type, NodePattern start, NodePattern end, boolean bidirectional, Map<String, Object> keyFilters, Set<String> includeProperties, Set<String> excludeProperties) {
        this.type = type;
        this.start = start;
        this.end = end;
        this.bidirectional = bidirectional;
        this.keyFilters = keyFilters;
        this.includeProperties = includeProperties;
        this.excludeProperties = excludeProperties;
    }

    public String getType() {
        return type;
    }

    public NodePattern getStart() {
        return start;
    }

    public NodePattern getEnd() {
        return end;
    }

    public boolean isBidirectional() {
        return bidirectional;
    }

    public Map<String, Object> getKeyFilters() {
        return keyFilters;
    }

    public Set<String> getIncludeProperties() {
        return includeProperties;
    }

    public Set<String> getExcludeProperties() {
        return excludeProperties;
    }

    @NotNull
    @Override
    public Set<Selector> toSelector() {
        return null;
    }
}
