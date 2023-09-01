package org.neo4j.cdc.client.model;

import java.util.Map;

public class State {

    private Map<String, Object> before;
    private Map<String, Object> after;

    public Map<String, Object> getBefore() {
        return this.before;
    }

    public Map<String, Object> getAfter() {
        return this.after;
    }
 }
