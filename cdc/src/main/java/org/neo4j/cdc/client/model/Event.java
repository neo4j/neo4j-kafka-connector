package org.neo4j.cdc.client.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "eventType",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = NodeEvent.class, name = "n"),
        @JsonSubTypes.Type(value = RelationshipEvent.class, name = "r"),
})
public abstract class Event {

    private String elementId;
    private String eventType;
    private State state;
    private String operation;

    public String getElementId() {
        return this.elementId;
    }

    public String getEventType() {
        return this.eventType;
    }

    public State getState() {
        return this.state;
    }

    public String getOperation() {
        return this.operation;
    }
}
