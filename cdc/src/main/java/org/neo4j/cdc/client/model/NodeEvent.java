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
package org.neo4j.cdc.client.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections4.MapUtils;

public class NodeEvent extends EntityEvent<NodeState> {

    private final Map<String, Map<String, Object>> keys;
    private final List<String> labels;

    @SuppressWarnings("unchecked")
    NodeEvent(Map<String, Object> map) {
        super(map, NodeState::new);

        this.keys = (Map<String, Map<String, Object>>) MapUtils.getObject(map, "keys");
        this.labels = (List<String>) MapUtils.getObject(map, "labels");
    }

    public Map<String, Map<String, Object>> getKeys() {
        return this.keys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        NodeEvent nodeEvent = (NodeEvent) o;

        if (!Objects.equals(keys, nodeEvent.keys)) return false;
        return Objects.equals(labels, nodeEvent.labels);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (keys != null ? keys.hashCode() : 0);
        result = 31 * result + (labels != null ? labels.hashCode() : 0);
        return result;
    }

    public List<String> getLabels() {
        return this.labels;
    }

    @Override
    public String toString() {
        return String.format(
                "NodeEvent{elementId=%s, labels=%s, keys=%s, operation=%s, before=%s, after=%s}",
                getElementId(), labels, keys, getOperation(), getBefore(), getAfter());
    }
}
