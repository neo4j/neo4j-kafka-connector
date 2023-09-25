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

public interface Event {

    EventType getEventType();

    static Event create(Map<?, ?> event) {
        var cypherMap = ModelUtils.checkedMap(Objects.requireNonNull(event), String.class, Object.class);
        var type = MapUtils.getString(cypherMap, "eventType");

        if (EventType.NODE.shorthand.equalsIgnoreCase(type)) {
            return NodeEvent.fromMap(event);
        } else if (EventType.RELATIONSHIP.shorthand.equalsIgnoreCase(type)) {
            return RelationshipEvent.fromMap(event);
        }

        throw new IllegalArgumentException(String.format("unknown event type: %s", type));
    }
}
