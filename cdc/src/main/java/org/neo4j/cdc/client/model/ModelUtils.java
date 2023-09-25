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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import org.apache.commons.collections4.MapUtils;

class ModelUtils {
    private ModelUtils() {}

    @SuppressWarnings("unchecked")
    static <K, V> Map<K, V> checkedMap(Map<?, ?> input, Class<K> keyType, Class<V> valueType) {
        if (input == null) {
            return null;
        }

        if (input.keySet().stream().anyMatch(k -> !keyType.isInstance(k))) {
            throw new IllegalArgumentException(String.format(
                    "There are keys of unsupported types in the provided map, expected: %s", keyType.getSimpleName()));
        }

        if (input.values().stream().anyMatch(v -> v != null && !valueType.isInstance(v))) {
            throw new IllegalArgumentException(String.format(
                    "There are values of unsupported types in the provided map, expected: %s",
                    valueType.getSimpleName()));
        }

        return (Map<K, V>) input;
    }

    @SuppressWarnings({"SameParameterValue"})
    static <K, V> Map<K, V> getMap(Map<String, Object> input, String key, Class<K> keyType, Class<V> valueType) {
        var value = input.get(key);
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            return checkedMap((Map<?, ?>) value, keyType, valueType);
        }

        throw new IllegalArgumentException(String.format(
                "Unsupported type %s, expected Map", value.getClass().getSimpleName()));
    }

    @SuppressWarnings({"unchecked", "SameParameterValue"})
    static <T> List<T> getList(Map<String, Object> input, String key, Class<T> type) {
        var value = MapUtils.getObject(input, key);
        if (value == null) {
            return null;
        }
        if (value instanceof List) {
            if (((List<?>) value).stream().anyMatch(v -> !type.isInstance(v))) {
                throw new IllegalArgumentException("There are elements of unsupported types in the provided list");
            }

            return (List<T>) value;
        }

        throw new IllegalArgumentException(String.format(
                "Unsupported type %s, expected List", value.getClass().getSimpleName()));
    }

    static ZonedDateTime getZonedDateTime(Map<String, Object> map, String key) {
        var value = MapUtils.getObject(map, key);
        if (value == null) {
            return null;
        }
        if (value instanceof TemporalAccessor) {
            return ZonedDateTime.from((TemporalAccessor) value);
        }
        return ZonedDateTime.parse(value.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX"));
    }
}
