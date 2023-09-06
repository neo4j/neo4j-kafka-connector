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
import java.util.function.Function;
import org.apache.commons.collections4.MapUtils;

public abstract class EntityEvent<T> implements Event {

    private final String elementId;
    private final String eventType;
    private final T before;
    private final T after;
    private final String operation;

    @SuppressWarnings("unchecked")
    protected EntityEvent(Map<String, Object> map, Function<Map<String, Object>, T> stateFactory) {
        this.elementId = Objects.requireNonNull(MapUtils.getString(map, "elementId"));
        this.eventType = Objects.requireNonNull(MapUtils.getString(map, "eventType"));
        this.operation = Objects.requireNonNull(MapUtils.getString(map, "operation"));

        var state = (Map<String, Object>) MapUtils.getMap(map, "state");
        var beforeMap = (Map<String, Object>) MapUtils.getMap(state, "before");
        this.before = beforeMap == null ? null : stateFactory.apply(beforeMap);
        var afterMap = (Map<String, Object>) MapUtils.getMap(state, "after");
        this.after = afterMap == null ? null : stateFactory.apply(afterMap);
    }

    public String getElementId() {
        return this.elementId;
    }

    public String getEventType() {
        return this.eventType;
    }

    public T getBefore() {
        return this.before;
    }

    public T getAfter() {
        return this.after;
    }

    public String getOperation() {
        return this.operation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntityEvent<?> that = (EntityEvent<?>) o;

        if (!elementId.equals(that.elementId)) return false;
        if (!eventType.equals(that.eventType)) return false;
        if (!Objects.equals(before, that.before)) return false;
        if (!Objects.equals(after, that.after)) return false;
        return operation.equals(that.operation);
    }

    @Override
    public int hashCode() {
        int result = elementId.hashCode();
        result = 31 * result + eventType.hashCode();
        result = 31 * result + (before != null ? before.hashCode() : 0);
        result = 31 * result + (after != null ? after.hashCode() : 0);
        result = 31 * result + operation.hashCode();
        return result;
    }
}
