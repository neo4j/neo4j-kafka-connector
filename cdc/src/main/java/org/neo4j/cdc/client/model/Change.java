package org.neo4j.cdc.client.model;

import java.util.Objects;

public enum Change {
    CREATE("c"),
    UPDATE("u"),
    DELETE("d");

    public final String shorthand;

    Change(String shorthand) {
        this.shorthand = Objects.requireNonNull(shorthand);
    }
}
