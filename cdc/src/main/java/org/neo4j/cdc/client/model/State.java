package org.neo4j.cdc.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class State {

    private final Map<String, Object> before;
    private final Map<String, Object> after;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public State(@JsonProperty("before") Map<String, Object> before,
                 @JsonProperty("after") Map<String, Object> after) {
        this.before = before;
        this.after = after;
    }

    public Map<String, Object> getBefore() {
        return this.before;
    }

    public Map<String, Object> getAfter() {
        return this.after;
    }
 }
