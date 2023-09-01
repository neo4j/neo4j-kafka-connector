package org.neo4j.cdc.client.selector;

import java.util.Map;
import java.util.Set;
import org.neo4j.cdc.client.model.Change;

public class RelationshipSelector extends EntitySelector {
    private final String type;
    private final RelationshipNodeSelector start;
    private final RelationshipNodeSelector end;
    private final Map<String, Object> key;

    public RelationshipSelector(
            Change change,
            Set<String> changesTo,
            String type,
            RelationshipNodeSelector start,
            RelationshipNodeSelector end,
            Map<String, Object> key,
            Set<String> includeProperties,
            Set<String> excludeProperties) {
        super(change, changesTo, includeProperties, excludeProperties);

        this.type = type;
        this.start = start;
        this.end = end;
        this.key = key;
    }

    public String getType() {
        return this.type;
    }

    public RelationshipNodeSelector getStart() {
        return this.start;
    }

    public RelationshipNodeSelector getEnd() {
        return this.end;
    }

    public Map<String, Object> getKey() {
        return this.key;
    }
}
