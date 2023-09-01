package org.neo4j.cdc.client.selector;

import org.neo4j.cdc.client.model.Change;

import java.util.List;
import java.util.Set;


public class EntitySelector implements Selector {
    private final Change change;
    private final Set<String> changesTo;
    private final Set<String> includeProperties;
    private final Set<String> excludeProperties;

    public EntitySelector(Change change, Set<String> changesTo, Set<String> includeProperties, Set<String> excludeProperties) {
        this.change = change;
        this.changesTo = changesTo;
        this.includeProperties = includeProperties;
        this.excludeProperties = excludeProperties;
    }

    public Change getChange() {
        return change;
    }

    public Set<String> getChangesTo() {
        return changesTo;
    }

    public Set<String> getIncludeProperties() {
        return includeProperties;
    }

    public Set<String> getExcludeProperties() {
        return excludeProperties;
    }
}
