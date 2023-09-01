package org.neo4j.cdc.client.selector;

import org.neo4j.cdc.client.model.Change;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class NodeSelector extends EntitySelector {
    private final Set<String> labels;
    private final Map<String, Object> key;

    public NodeSelector(Change change, Set<String> changesTo, Set<String> labels, Map<String, Object> key, Set<String> includeProperties, Set<String> excludeProperties) {
        super(change, changesTo, includeProperties, excludeProperties);

        this.labels = labels;
        this.key = key;
    }

    public Set<String> getLabels() {
        return labels;
    }

    public Map<String, Object> getKey() {
        return key;
    }
}
