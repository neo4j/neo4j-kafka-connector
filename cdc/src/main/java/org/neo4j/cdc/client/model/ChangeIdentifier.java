package org.neo4j.cdc.client.model;

public class ChangeIdentifier {
    private final String id;

    public ChangeIdentifier(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }
}
