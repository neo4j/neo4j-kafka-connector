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
import org.neo4j.cdc.client.model.ChangeEvent;

public class RelationshipNodeSelector implements Selector {
    @NotNull
    private final Set<String> labels;

    @NotNull
    private final Map<String, Object> key;

    public RelationshipNodeSelector() {
        this(emptySet(), emptyMap());
    }

    public RelationshipNodeSelector(@NotNull Set<String> labels, @NotNull Map<String, Object> key) {
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
    public Map<String, Object> asMap() {
        var result = new HashMap<String, Object>();

        if (!labels.isEmpty()) {
            result.put("labels", labels);
        }
        if (!key.isEmpty()) {
            result.put("key", key);
        }

        return result;
    }

    public boolean isEmpty() {
        return labels.isEmpty() && key.isEmpty();
    }

    @Override
    public boolean matches(ChangeEvent e) {
        throw new UnsupportedOperationException("not supported on relationship node selectors");
    }

    @Override
    public ChangeEvent applyProperties(ChangeEvent e) {
        throw new UnsupportedOperationException("not supported on relationship node selectors");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationshipNodeSelector that = (RelationshipNodeSelector) o;

        if (!labels.equals(that.labels)) return false;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = labels.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }
}
