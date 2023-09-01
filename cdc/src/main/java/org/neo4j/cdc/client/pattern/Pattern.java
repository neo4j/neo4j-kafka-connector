package org.neo4j.cdc.client.pattern;

import java.util.List;
import java.util.Set;
import org.neo4j.cdc.client.selector.Selector;

public interface Pattern {

    Set<Selector> toSelector();

    static List<Pattern> parse(String expression) {
        return Visitors.parse(expression);
    }
}
