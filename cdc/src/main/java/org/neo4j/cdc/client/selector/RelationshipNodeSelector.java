package org.neo4j.cdc.client.selector;

import java.util.Map;
import java.util.Set;

public class RelationshipNodeSelector {
    private final Set<String> labels;
    private final Map<String, Object> key;

    public RelationshipNodeSelector(Set<String> labels, Map<String, Object> key) {
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
