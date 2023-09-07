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

public class Node {

    private final String elementId;
    private final Map<String, Object> keys;
    private final List<String> labels;

    public Node(String elementId, List<String> labels, Map<String, Object> keys) {
        this.elementId = Objects.requireNonNull(elementId);
        this.labels = labels;
        this.keys = keys;
    }

    public String getElementId() {
        return this.elementId;
    }

    public Map<String, Object> getKeys() {
        return this.keys;
    }

    public List<String> getLabels() {
        return this.labels;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        if (!elementId.equals(node.elementId)) return false;
        if (!Objects.equals(keys, node.keys)) return false;
        return Objects.equals(labels, node.labels);
    }

    @Override
    public int hashCode() {
        int result = elementId.hashCode();
        result = 31 * result + (keys != null ? keys.hashCode() : 0);
        result = 31 * result + (labels != null ? labels.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("Node{elementId=%s, labels=%s, keys=%s}", elementId, labels, keys);
    }

    public static Node fromMap(Map<?, ?> map) {
        var cypherMap = ModelUtils.checkedMap(Objects.requireNonNull(map), String.class, Object.class);

        var elementId = MapUtils.getString(cypherMap, "elementId");
        var labels = ModelUtils.getList(cypherMap, "labels", String.class);
        var keys = ModelUtils.getMap(cypherMap, "keys", String.class, Object.class);

        return new Node(elementId, labels, keys);
    }
}
