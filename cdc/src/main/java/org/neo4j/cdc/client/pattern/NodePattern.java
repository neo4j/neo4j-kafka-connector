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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.neo4j.cdc.client.selector.NodeSelector;
import org.neo4j.cdc.client.selector.Selector;

public class NodePattern implements Pattern {
    @NotNull
    private final Set<String> labels;

    @NotNull
    private final Map<String, Object> keyFilters;

    @NotNull
    private final Set<String> includeProperties;

    @NotNull
    private final Set<String> excludeProperties;

    public NodePattern(
            @NotNull Set<String> labels,
            @NotNull Map<String, Object> keyFilters,
            @NotNull Set<String> includeProperties,
            @NotNull Set<String> excludeProperties) {
        this.labels = Objects.requireNonNull(labels);
        this.keyFilters = Objects.requireNonNull(keyFilters);
        this.includeProperties = Objects.requireNonNull(includeProperties);
        this.excludeProperties = Objects.requireNonNull(excludeProperties);
    }

    public @NotNull Set<String> getLabels() {
        return labels;
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

        NodePattern that = (NodePattern) o;

        if (!Objects.equals(labels, that.labels)) return false;
        if (!Objects.equals(keyFilters, that.keyFilters)) return false;
        if (!Objects.equals(includeProperties, that.includeProperties)) return false;
        return Objects.equals(excludeProperties, that.excludeProperties);
    }

    @Override
    public int hashCode() {
        int result = labels.hashCode();
        result = 31 * result + keyFilters.hashCode();
        result = 31 * result + includeProperties.hashCode();
        result = 31 * result + excludeProperties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "NodePattern{" + "labels="
                + labels + ", keyFilters="
                + keyFilters + ", includeProperties="
                + includeProperties + ", excludeProperties="
                + excludeProperties + '}';
    }

    @NotNull
    @Override
    public Set<Selector> toSelector() {
        return Set.of(new NodeSelector(
                null, Collections.emptySet(), labels, keyFilters, includeProperties, excludeProperties));
    }
}
