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

public class EntitySelector implements Selector {
    private final EntityOperation change;
    private final Set<String> changesTo;
    private final Set<String> includeProperties;
    private final Set<String> excludeProperties;

    public EntitySelector(
            EntityOperation change,
            Set<String> changesTo,
            Set<String> includeProperties,
            Set<String> excludeProperties) {
        this.change = change;
        this.changesTo = changesTo;
        this.includeProperties = includeProperties;
        this.excludeProperties = excludeProperties;
    }

    public EntityOperation getChange() {
        return change;
    }

    public Set<String> getChangesTo() {
        return changesTo;
    }

    public Set<String> getIncludeProperties() {
        return includeProperties;
    }

    public Set<String> getExcludeProperties() {
        return excludeProperties;
    }

    @Override
    public Map<String, Object> asMap() {
        var result = new HashMap<String, Object>();

        result.put("select", "e");
        if (change != null) {
            result.put("operation", change.shorthand);
        }
        if (changesTo != null && !changesTo.isEmpty()) {
            result.put("changesTo", changesTo);
        }

        return result;
    }
}
