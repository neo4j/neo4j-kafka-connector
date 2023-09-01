package org.neo4j.cdc.client.model;

import java.util.List;
import java.util.Map;

public class RelationshipEvent extends Event {

    private Node start;
    private Node end;
    private String type;
    private Map<String, Object> key;
    private List<String> labels;

    public Node getStart() {
        return this.start;
    }

    public Node getEnd() {
        return this.end;
    }

    public String getType() {
        return this.type;
    }

    public Map<String, Object> getKey() {
        return this.key;
    }

    public List<String> getLabels() {
        return this.labels;
    }
}
