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

public class RelationshipNodeSelector implements Selector {
    private final Set<String> labels;
    private final Map<String, Object> key;

    public RelationshipNodeSelector(Set<String> labels, Map<String, Object> key) {
        this.labels = labels;
        this.key = key;
    }

    public Set<String> getLabels() {
        return labels;
    }

    public Map<String, Object> getKey() {
        return key;
    }

    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<String, Object>();

        if (labels != null && !labels.isEmpty()) {
            result.put("labels", labels);
        }
        if (key != null && !key.isEmpty()) {
            result.put("key", key);
        }

        return result;
    }
}
