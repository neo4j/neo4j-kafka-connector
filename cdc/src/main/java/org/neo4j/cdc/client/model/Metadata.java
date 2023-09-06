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
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Metadata implements Map<String, Object> {
    public static final String EXECUTING_USER = "executingUser";
    public static final String CONNECTION_CLIENT = "connectionClient";
    public static final String AUTHENTICATED_USER = "authenticatedUser";
    public static final String CAPTURE_MODE = "captureMode";
    public static final String SERVER_ID = "serverId";
    public static final String CONNECTION_TYPE = "connectionType";
    public static final String CONNECTION_SERVER = "connectionServer";
    public static final String TX_START_TIME = "txStartTime";
    public static final String TX_COMMIT_TIME = "txCommitTime";
    public static final String[] KNOWN_KEYS = new String[] {
        EXECUTING_USER,
        AUTHENTICATED_USER,
        CONNECTION_TYPE,
        CONNECTION_CLIENT,
        CONNECTION_SERVER,
        CAPTURE_MODE,
        SERVER_ID,
        TX_COMMIT_TIME,
        TX_START_TIME
    };

    private final Map<String, Object> map;

    public Metadata(Map<String, Object> map) {
        this.map = Objects.requireNonNull(map);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return map.get(key);
    }

    @Nullable
    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@NotNull Map<? extends String, ?> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        return map.keySet();
    }

    @NotNull
    @Override
    public Collection<Object> values() {
        return map.values();
    }

    @NotNull
    @Override
    public Set<Entry<String, Object>> entrySet() {
        return map.entrySet();
    }

    public String getExecutingUser() {
        return getString(EXECUTING_USER);
    }

    public String getConnectionClient() {
        return getString(CONNECTION_CLIENT);
    }

    public String getAuthenticatedUser() {
        return getString(AUTHENTICATED_USER);
    }

    public CaptureMode getCaptureMode() {
        return getEnum(CAPTURE_MODE, CaptureMode.class, CaptureMode.FULL);
    }

    public String getServerId() {
        return getString(SERVER_ID);
    }

    public String getConnectionType() {
        return getString(CONNECTION_TYPE);
    }

    public String getConnectionServer() {
        return getString(CONNECTION_SERVER);
    }

    public ZonedDateTime getTxStartTime() {
        return getDateTime(TX_START_TIME);
    }

    public ZonedDateTime getTxCommitTime() {
        return getDateTime(TX_COMMIT_TIME);
    }

    private String getString(String key) {
        var value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        return value.toString();
    }

    private ZonedDateTime getDateTime(String key) {
        var value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof TemporalAccessor) {
            return ZonedDateTime.from((TemporalAccessor) value);
        }
        return ZonedDateTime.parse(value.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX"));
    }

    @SuppressWarnings("SameParameterValue")
    private <T extends Enum<T>> T getEnum(String key, Class<T> enumType, T defaultValue) {
        var value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        return Enum.valueOf(enumType, value.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Metadata metadata = (Metadata) o;

        return map.equals(metadata.map);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return String.format("Metadata{%s}", map);
    }
}
