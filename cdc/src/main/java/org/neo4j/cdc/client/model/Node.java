package org.neo4j.cdc.client.model;

import java.util.List;
import java.util.Map;

public class Node {

    private String elementId;
    private Map<String, Object> keys;
    private List<String> labels;

    public String getElementId() {
        return this.elementId;
    }

    public Map<String, Object> getKeys() {
        return this.keys;
    }

    public List<String> getLabels() {
        return this.labels;
    }
}
