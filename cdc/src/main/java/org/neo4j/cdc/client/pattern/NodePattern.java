package org.neo4j.cdc.client.pattern;

import org.jetbrains.annotations.NotNull;
import org.neo4j.cdc.client.selector.Selector;

import java.util.Map;
import java.util.Set;

public class NodePattern implements Pattern {
    private final Set<String> labels;
    private final Map<String, Object> keyFilters;
    private final Set<String> includeProperties;
    private final Set<String> excludeProperties;

    public NodePattern(Set<String> labels, Map<String, Object> keyFilters, Set<String> includeProperties, Set<String> excludeProperties) {
        this.labels = labels;
        this.keyFilters = keyFilters;
        this.includeProperties = includeProperties;
        this.excludeProperties = excludeProperties;
    }

    public Set<String> getLabels() {
        return labels;
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
