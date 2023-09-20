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
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.EntityOperation;
import org.neo4j.cdc.client.model.NodeEvent;

public class NodeSelector extends EntitySelector {
    @NotNull
    private final Set<String> labels;

    @NotNull
    private final Map<String, Object> key;

    public NodeSelector() {
        this(null);
    }

    public NodeSelector(@Nullable EntityOperation change) {
        this(change, emptySet());
    }

    public NodeSelector(@Nullable EntityOperation change, @NotNull Set<String> changesTo) {
        this(change, changesTo, emptySet());
    }

    public NodeSelector(@Nullable EntityOperation change, @NotNull Set<String> changesTo, @NotNull Set<String> labels) {
        this(change, changesTo, labels, emptyMap());
    }

    public NodeSelector(
            @Nullable EntityOperation change,
            @NotNull Set<String> changesTo,
            @NotNull Set<String> labels,
            @NotNull Map<String, Object> key) {
        this(change, changesTo, labels, key, emptySet(), emptySet());
    }

    public NodeSelector(
            @Nullable EntityOperation change,
            @NotNull Set<String> changesTo,
            @NotNull Set<String> labels,
            @NotNull Map<String, Object> key,
            @NotNull Set<String> includeProperties,
            @NotNull Set<String> excludeProperties) {
        super(change, changesTo, includeProperties, excludeProperties);

        this.labels = Objects.requireNonNull(labels);
        this.key = Objects.requireNonNull(key);
    }

    public @NotNull Set<String> getLabels() {
        return labels;
    }

    public @NotNull Map<String, Object> getKey() {
        return key;
    }

    @Override
    public boolean matches(ChangeEvent e) {
        if (!(e.getEvent() instanceof NodeEvent)) {
            return false;
        }

        if (!super.matches(e)) {
            return false;
        }

        NodeEvent nodeEvent = (NodeEvent) e.getEvent();
        if (labels.stream().anyMatch(l -> !nodeEvent.getLabels().contains(l))) {
            return false;
        }

        if (!key.isEmpty() && !nodeEvent.getKeys().containsValue(key)) {
            return false;
        }

        return true;
    }

    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<>(super.asMap());

        result.put("select", "n");
        if (!labels.isEmpty()) {
            result.put("labels", labels);
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

        NodeSelector that = (NodeSelector) o;

        if (!labels.equals(that.labels)) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + labels.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }
}
