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

public class RelationshipState {
    private final Map<String, Object> properties;

    @SuppressWarnings("unchecked")
    public RelationshipState(Map<String, Object> map) {
        this.properties = (Map<String, Object>) MapUtils.getMap(map, "properties");
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationshipState that = (RelationshipState) o;

        return Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return properties != null ? properties.hashCode() : 0;
    }

    @Override
    public String toString() {
        return String.format("RelationshipState{properties=%s}", properties);
    }
}
