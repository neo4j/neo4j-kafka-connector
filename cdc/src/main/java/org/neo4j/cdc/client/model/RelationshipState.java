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

public class RelationshipState extends State {

    public RelationshipState(Map<String, Object> properties) {
        super(properties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationshipState that = (RelationshipState) o;

        return Objects.equals(getProperties(), that.getProperties());
    }

    @Override
    public int hashCode() {
        return getProperties() != null ? getProperties().hashCode() : 0;
    }

    @Override
    public String toString() {
        return String.format("RelationshipState{properties=%s}", getProperties());
    }

    public static RelationshipState fromMap(Map<?, ?> map) {
        if (map == null) {
            return null;
        }

        var cypherMap = ModelUtils.checkedMap(map, String.class, Object.class);
        var properties = ModelUtils.checkedMap(MapUtils.getMap(cypherMap, "properties"), String.class, Object.class);
        return new RelationshipState(properties);
    }
}
