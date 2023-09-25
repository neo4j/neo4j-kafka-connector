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
package org.neo4j.cdc.client.selector;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import java.util.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cdc.client.model.*;

public class RelationshipSelector extends EntitySelector {
    @Nullable
    private final String type;

    @NotNull
    private final RelationshipNodeSelector start;

    @NotNull
    private final RelationshipNodeSelector end;

    @NotNull
    private final Map<String, Object> key;

    public RelationshipSelector() {
        this(null);
    }

    public RelationshipSelector(EntityOperation change) {
        this(change, emptySet());
    }

    public RelationshipSelector(EntityOperation change, Set<String> changesTo) {
        this(change, changesTo, null);
    }

    public RelationshipSelector(EntityOperation change, Set<String> changesTo, String type) {
        this(change, changesTo, type, new RelationshipNodeSelector());
    }

    public RelationshipSelector(
            EntityOperation change, Set<String> changesTo, String type, RelationshipNodeSelector start) {
        this(change, changesTo, type, start, new RelationshipNodeSelector());
    }

    public RelationshipSelector(
            EntityOperation change,
            Set<String> changesTo,
            String type,
            RelationshipNodeSelector start,
            RelationshipNodeSelector end) {
        this(change, changesTo, type, start, end, emptyMap());
    }

    public RelationshipSelector(
            EntityOperation change,
            Set<String> changesTo,
            String type,
            RelationshipNodeSelector start,
            RelationshipNodeSelector end,
            Map<String, Object> key) {
        this(change, changesTo, type, start, end, key, emptySet(), emptySet());
    }

    public RelationshipSelector(
            @Nullable EntityOperation change,
            @NotNull Set<String> changesTo,
            @Nullable String type,
            @NotNull RelationshipNodeSelector start,
            @NotNull RelationshipNodeSelector end,
            @NotNull Map<String, Object> key,
            @NotNull Set<String> includeProperties,
            @NotNull Set<String> excludeProperties) {
        super(change, changesTo, includeProperties, excludeProperties);

        this.type = type;
        this.start = Objects.requireNonNull(start);
        this.end = Objects.requireNonNull(end);
        this.key = Objects.requireNonNull(key);
    }

    public @Nullable String getType() {
        return this.type;
    }

    public @NotNull RelationshipNodeSelector getStart() {
        return this.start;
    }

    public @NotNull RelationshipNodeSelector getEnd() {
        return this.end;
    }

    public @NotNull Map<String, Object> getKey() {
        return this.key;
    }

    @Override
    public boolean matches(ChangeEvent e) {
        if (!(e.getEvent() instanceof RelationshipEvent)) {
            return false;
        }

        if (!super.matches(e)) {
            return false;
        }

        RelationshipEvent relationshipEvent = (RelationshipEvent) e.getEvent();
        if (type != null && !relationshipEvent.getType().equals(type)) {
            return false;
        }

        if (start.getLabels().stream()
                        .anyMatch(l -> !relationshipEvent.getStart().getLabels().contains(l))
                || (!start.getKey().isEmpty()
                        && !relationshipEvent.getStart().getKeys().containsValue(start.getKey()))) {
            return false;
        }

        if (end.getLabels().stream()
                        .anyMatch(l -> !relationshipEvent.getEnd().getLabels().contains(l))
                || (!end.getKey().isEmpty()
                        && !relationshipEvent.getEnd().getKeys().containsValue(end.getKey()))) {
            return false;
        }

        if (!key.isEmpty() && !Objects.equals(key, relationshipEvent.getKey())) {
            return false;
        }

        return true;
    }

    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<>(super.asMap());

        result.put("select", "r");
        if (type != null) {
            result.put("type", type);
        }
        if (!start.isEmpty()) {
            result.put("start", start.asMap());
        }
        if (!end.isEmpty()) {
            result.put("end", end.asMap());
        }
        if (!key.isEmpty()) {
            result.put("key", key);
        }

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RelationshipSelector that = (RelationshipSelector) o;

        if (!Objects.equals(type, that.type)) return false;
        if (!start.equals(that.start)) return false;
        if (!end.equals(that.end)) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + start.hashCode();
        result = 31 * result + end.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }
}
