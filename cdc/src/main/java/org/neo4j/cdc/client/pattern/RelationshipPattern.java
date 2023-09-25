/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cdc.client.pattern;

import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cdc.client.selector.RelationshipNodeSelector;
import org.neo4j.cdc.client.selector.RelationshipSelector;
import org.neo4j.cdc.client.selector.Selector;

public class RelationshipPattern implements Pattern {
    @Nullable
    private final String type;

    @NotNull
    private final NodePattern start;

    @NotNull
    private final NodePattern end;

    private final boolean bidirectional;

    @NotNull
    private final Map<String, Object> keyFilters;

    @NotNull
    private final Set<String> includeProperties;

    @NotNull
    private final Set<String> excludeProperties;

    public RelationshipPattern(
            @Nullable String type,
            @NotNull NodePattern start,
            @NotNull NodePattern end,
            boolean bidirectional,
            @NotNull Map<String, Object> keyFilters,
            @NotNull Set<String> includeProperties,
            @NotNull Set<String> excludeProperties) {
        this.type = type;
        this.start = start;
        this.end = end;
        this.bidirectional = bidirectional;
        this.keyFilters = keyFilters;
        this.includeProperties = includeProperties;
        this.excludeProperties = excludeProperties;
    }

    public @Nullable String getType() {
        return type;
    }

    public @NotNull NodePattern getStart() {
        return start;
    }

    public @NotNull NodePattern getEnd() {
        return end;
    }

    public boolean isBidirectional() {
        return bidirectional;
    }

    public @NotNull Map<String, Object> getKeyFilters() {
        return keyFilters;
    }

    public @NotNull Set<String> getIncludeProperties() {
        return includeProperties;
    }

    public @NotNull Set<String> getExcludeProperties() {
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
        result = 31 * result + start.hashCode();
        result = 31 * result + end.hashCode();
        result = 31 * result + (bidirectional ? 1 : 0);
        result = 31 * result + keyFilters.hashCode();
        result = 31 * result + includeProperties.hashCode();
        result = 31 * result + excludeProperties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RelationshipPattern{" + "type='"
                + type + '\'' + ", start="
                + start + ", end="
                + end + ", bidirectional="
                + bidirectional + ", keyFilters="
                + keyFilters + ", includeProperties="
                + includeProperties + ", excludeProperties="
                + excludeProperties + '}';
    }

    @NotNull
    @Override
    public Set<Selector> toSelector() {
        var result = new HashSet<Selector>();

        result.add(new RelationshipSelector(
                null,
                Collections.emptySet(),
                type,
                new RelationshipNodeSelector(start.getLabels(), start.getKeyFilters()),
                new RelationshipNodeSelector(end.getLabels(), end.getKeyFilters()),
                keyFilters,
                includeProperties,
                excludeProperties));

        if (bidirectional) {
            result.add(new RelationshipSelector(
                    null,
                    Collections.emptySet(),
                    type,
                    new RelationshipNodeSelector(end.getLabels(), end.getKeyFilters()),
                    new RelationshipNodeSelector(start.getLabels(), start.getKeyFilters()),
                    keyFilters,
                    includeProperties,
                    excludeProperties));
        }

        return result;
    }
}
