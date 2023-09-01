package org.neo4j.cdc.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class Node {

    private final String elementId;
    private final Map<String, Object> keys;
    private final List<String> labels;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Node(@JsonProperty("elementId") String elementId,
                @JsonProperty("keys") Map<String, Object> keys,
                @JsonProperty("labels") List<String> labels) {
        this.elementId = elementId;
        this.keys = keys;
        this.labels = labels;
    }

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
