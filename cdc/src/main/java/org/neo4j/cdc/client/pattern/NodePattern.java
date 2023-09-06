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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.neo4j.cdc.client.selector.NodeSelector;
import org.neo4j.cdc.client.selector.Selector;

public class NodePattern implements Pattern {
    private final Set<String> labels;
    private final Map<String, Object> keyFilters;
    private final Set<String> includeProperties;
    private final Set<String> excludeProperties;

    public NodePattern(
            Set<String> labels,
            Map<String, Object> keyFilters,
            Set<String> includeProperties,
            Set<String> excludeProperties) {
        this.labels = labels;
        this.keyFilters = keyFilters;
        this.includeProperties = includeProperties;
        this.excludeProperties = excludeProperties;
    }

    public Set<String> getLabels() {
        return labels;
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

        NodePattern that = (NodePattern) o;

        if (!Objects.equals(labels, that.labels)) return false;
        if (!Objects.equals(keyFilters, that.keyFilters)) return false;
        if (!Objects.equals(includeProperties, that.includeProperties)) return false;
        return Objects.equals(excludeProperties, that.excludeProperties);
    }

    @Override
    public int hashCode() {
        int result = labels != null ? labels.hashCode() : 0;
        result = 31 * result + (keyFilters != null ? keyFilters.hashCode() : 0);
        result = 31 * result + (includeProperties != null ? includeProperties.hashCode() : 0);
        result = 31 * result + (excludeProperties != null ? excludeProperties.hashCode() : 0);
        return result;
    }

    @NotNull
    @Override
    public Set<Selector> toSelector() {
        return Set.of(new NodeSelector(null, null, labels, keyFilters, includeProperties, excludeProperties));
    }
}
