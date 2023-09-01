package org.neo4j.cdc.client.pattern;

import org.neo4j.cdc.client.selector.Selector;

import java.util.List;
import java.util.Set;

public interface Pattern {

    Set<Selector> toSelector();

    static List<Pattern> parse(String expression) {
        return List.of();
    }

}
