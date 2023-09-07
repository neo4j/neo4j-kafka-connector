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
package org.neo4j.cdc.client.selector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.neo4j.cdc.client.model.EntityOperation;

public class RelationshipSelector extends EntitySelector {
    private final String type;
    private final RelationshipNodeSelector start;
    private final RelationshipNodeSelector end;
    private final Map<String, Object> key;

    public RelationshipSelector(
            EntityOperation change,
            Set<String> changesTo,
            String type,
            RelationshipNodeSelector start,
            RelationshipNodeSelector end,
            Map<String, Object> key,
            Set<String> includeProperties,
            Set<String> excludeProperties) {
        super(change, changesTo, includeProperties, excludeProperties);

        this.type = type;
        this.start = start;
        this.end = end;
        this.key = key;
    }

    public String getType() {
        return this.type;
    }

    public RelationshipNodeSelector getStart() {
        return this.start;
    }

    public RelationshipNodeSelector getEnd() {
        return this.end;
    }

    public Map<String, Object> getKey() {
        return this.key;
    }

    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<>(super.asMap());

        result.put("select", "r");
        if (type != null) {
            result.put("type", type);
        }
        if (start != null) {
            result.put("start", start.asMap());
        }
        if (end != null) {
            result.put("end", end.asMap());
        }
        if (key != null && !key.isEmpty()) {
            result.put("key", key);
        }

        return result;
    }
}
