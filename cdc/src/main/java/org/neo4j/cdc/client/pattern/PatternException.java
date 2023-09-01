package org.neo4j.cdc.client.pattern;

public class PatternException extends Exception {

    public PatternException(String message) {
        super(message);
    }

    public PatternException(String message, Throwable cause) {
        super(message, cause);
    }

}
