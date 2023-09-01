package org.neo4j.cdc.client.model;

import java.util.List;
import java.util.Map;

public class NodeEvent extends Event {

    private Map<String, Object> keys;
    private List<String> labels;

    public Map<String, Object> getKeys() {
        return this.keys;
    }

    public List<String> getLabels() {
        return this.labels;
    }
}
