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
import org.apache.commons.collections4.MapUtils;

public interface Event {

    String getEventType();

    static Event create(Map<String, Object> event) {
        var type = MapUtils.getString(event, "eventType");

        switch (type) {
            case "n":
            case "N":
                return new NodeEvent(event);
            case "r":
            case "R":
                return new RelationshipEvent(event);
            default:
                throw new IllegalArgumentException(String.format("unknown event type: %s", type));
        }
    }
}
