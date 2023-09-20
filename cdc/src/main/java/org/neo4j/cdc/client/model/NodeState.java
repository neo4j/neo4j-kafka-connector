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

public class NodeState extends State {
    private final List<String> labels;

    public NodeState(List<String> labels, Map<String, Object> properties) {
        super(properties);

        this.labels = labels;
    }

    public List<String> getLabels() {
        return labels;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeState nodeState = (NodeState) o;

        if (!Objects.equals(labels, nodeState.labels)) return false;
        return Objects.equals(getProperties(), nodeState.getProperties());
    }

    @Override
    public int hashCode() {
        int result = labels != null ? labels.hashCode() : 0;
        result = 31 * result + (getProperties() != null ? getProperties().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("NodeState{labels=%s, properties=%s}", labels, getProperties());
    }

    public static NodeState fromMap(Map<?, ?> map) {
        if (map == null) {
            return null;
        }

        var cypherMap = ModelUtils.checkedMap(map, String.class, Object.class);
        var labels = ModelUtils.getList(cypherMap, "labels", String.class);
        var properties = ModelUtils.checkedMap(MapUtils.getMap(cypherMap, "properties"), String.class, Object.class);
        return new NodeState(labels, properties);
    }
}
