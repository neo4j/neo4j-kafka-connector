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

import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections4.MapUtils;

public class RelationshipEvent extends EntityEvent<RelationshipState> {

    private final Node start;
    private final Node end;
    private final String type;
    private final Map<String, Object> key;

    @SuppressWarnings("unchecked")
    RelationshipEvent(Map<String, Object> map) {
        super(map, RelationshipState::new);

        this.start = new Node(Objects.requireNonNull((Map<String, Object>) MapUtils.getMap(map, "start")));
        this.end = new Node(Objects.requireNonNull((Map<String, Object>) MapUtils.getMap(map, "end")));
        this.type = Objects.requireNonNull(MapUtils.getString(map, "type"));
        this.key = (Map<String, Object>) MapUtils.getMap(map, "key");
    }

    public Node getStart() {
        return this.start;
    }

    public Node getEnd() {
        return this.end;
    }

    public String getType() {
        return this.type;
    }

    public Map<String, Object> getKey() {
        return this.key;
    }

    @Override
    public String toString() {
        return String.format(
                "RelationshipEvent{elementId=%s, start=%s, end=%s, type='%s', key=%s, operation=%s, before=%s, after=%s}",
                getElementId(), start, end, type, key, getOperation(), getBefore(), getAfter());
    }
}
