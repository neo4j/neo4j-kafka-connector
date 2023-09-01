package org.neo4j.cdc.client.pattern;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.neo4j.cdc.client.selector.RelationshipNodeSelector;
import org.neo4j.cdc.client.selector.RelationshipSelector;
import org.neo4j.cdc.client.selector.Selector;

public class RelationshipPattern implements Pattern {
    private final String type;
    private final NodePattern start;
    private final NodePattern end;
    private final boolean bidirectional;
    private final Map<String, Object> keyFilters;
    private final Set<String> includeProperties;
    private final Set<String> excludeProperties;

    public RelationshipPattern(
            String type,
            NodePattern start,
            NodePattern end,
            boolean bidirectional,
            Map<String, Object> keyFilters,
            Set<String> includeProperties,
            Set<String> excludeProperties) {
        this.type = type;
        this.start = start;
        this.end = end;
        this.bidirectional = bidirectional;
        this.keyFilters = keyFilters;
        this.includeProperties = includeProperties;
        this.excludeProperties = excludeProperties;
    }

    public String getType() {
        return type;
    }

    public NodePattern getStart() {
        return start;
    }

    public NodePattern getEnd() {
        return end;
    }

    public boolean isBidirectional() {
        return bidirectional;
    }

    public Map<String, Object> getKeyFilters() {
        return keyFilters;
    }

    public Set<String> getIncludeProperties() {
        return includeProperties;
    }

    public Set<String> getExcludeProperties() {
        return excludeProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationshipPattern that = (RelationshipPattern) o;

        if (bidirectional != that.bidirectional) return false;
        if (!Objects.equals(type, that.type)) return false;
        if (!Objects.equals(start, that.start)) return false;
        if (!Objects.equals(end, that.end)) return false;
        if (!Objects.equals(keyFilters, that.keyFilters)) return false;
        if (!Objects.equals(includeProperties, that.includeProperties)) return false;
        return Objects.equals(excludeProperties, that.excludeProperties);
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (start != null ? start.hashCode() : 0);
        result = 31 * result + (end != null ? end.hashCode() : 0);
        result = 31 * result + (bidirectional ? 1 : 0);
        result = 31 * result + (keyFilters != null ? keyFilters.hashCode() : 0);
        result = 31 * result + (includeProperties != null ? includeProperties.hashCode() : 0);
        result = 31 * result + (excludeProperties != null ? excludeProperties.hashCode() : 0);
        return result;
    }

    @NotNull
    @Override
    public Set<Selector> toSelector() {
        var result = new HashSet<Selector>();

        result.add(new RelationshipSelector(
                null,
                null,
                type,
                start != null ? new RelationshipNodeSelector(start.getLabels(), start.getKeyFilters()) : null,
                end != null ? new RelationshipNodeSelector(end.getLabels(), end.getKeyFilters()) : null,
                keyFilters,
                includeProperties,
                excludeProperties));

        if (bidirectional) {
            result.add(new RelationshipSelector(
                    null,
                    null,
                    type,
                    end != null ? new RelationshipNodeSelector(end.getLabels(), end.getKeyFilters()) : null,
                    start != null ? new RelationshipNodeSelector(start.getLabels(), start.getKeyFilters()) : null,
                    keyFilters,
                    includeProperties,
                    excludeProperties));
        }

        return result;
    }
}
