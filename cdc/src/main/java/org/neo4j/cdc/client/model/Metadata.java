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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;

public class Metadata {
    private static final String EXECUTING_USER = "executingUser";
    private static final String CONNECTION_CLIENT = "connectionClient";
    private static final String AUTHENTICATED_USER = "authenticatedUser";
    private static final String CAPTURE_MODE = "captureMode";
    private static final String SERVER_ID = "serverId";
    private static final String CONNECTION_TYPE = "connectionType";
    private static final String CONNECTION_SERVER = "connectionServer";
    private static final String TX_START_TIME = "txStartTime";
    private static final String TX_COMMIT_TIME = "txCommitTime";
    private static final List<String> KNOWN_KEYS = List.of(
            EXECUTING_USER,
            AUTHENTICATED_USER,
            CONNECTION_TYPE,
            CONNECTION_CLIENT,
            CONNECTION_SERVER,
            CAPTURE_MODE,
            SERVER_ID,
            TX_COMMIT_TIME,
            TX_START_TIME);

    private final String executingUser;
    private final String connectionClient;
    private final String authenticatedUser;
    private final CaptureMode captureMode;
    private final String serverId;
    private final String connectionType;
    private final String connectionServer;
    private final ZonedDateTime txStartTime;
    private final ZonedDateTime txCommitTime;
    private final Map<String, Object> additionalEntries;

    public Metadata(
            String authenticatedUser,
            String executingUser,
            String serverId,
            CaptureMode captureMode,
            String connectionType,
            String connectionClient,
            String connectionServer,
            ZonedDateTime txStartTime,
            ZonedDateTime txCommitTime,
            Map<String, Object> additionalEntries) {
        this.executingUser = executingUser;
        this.connectionClient = connectionClient;
        this.authenticatedUser = authenticatedUser;
        this.captureMode = Objects.requireNonNull(captureMode);
        this.serverId = Objects.requireNonNull(serverId);
        this.connectionType = Objects.requireNonNull(connectionType);
        this.connectionServer = connectionServer;
        this.txStartTime = Objects.requireNonNull(txStartTime);
        this.txCommitTime = Objects.requireNonNull(txCommitTime);
        this.additionalEntries = additionalEntries;
    }

    public String getExecutingUser() {
        return this.executingUser;
    }

    public String getConnectionClient() {
        return this.connectionClient;
    }

    public String getAuthenticatedUser() {
        return this.authenticatedUser;
    }

    public CaptureMode getCaptureMode() {
        return this.captureMode;
    }

    public String getServerId() {
        return this.serverId;
    }

    public String getConnectionType() {
        return this.connectionType;
    }

    public String getConnectionServer() {
        return this.connectionServer;
    }

    public ZonedDateTime getTxStartTime() {
        return this.txStartTime;
    }

    public ZonedDateTime getTxCommitTime() {
        return this.txCommitTime;
    }

    public Map<String, Object> getAdditionalEntries() {
        return additionalEntries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Metadata metadata = (Metadata) o;

        if (!Objects.equals(executingUser, metadata.executingUser)) return false;
        if (!Objects.equals(connectionClient, metadata.connectionClient)) return false;
        if (!Objects.equals(authenticatedUser, metadata.authenticatedUser)) return false;
        if (captureMode != metadata.captureMode) return false;
        if (!serverId.equals(metadata.serverId)) return false;
        if (!connectionType.equals(metadata.connectionType)) return false;
        if (!Objects.equals(connectionServer, metadata.connectionServer)) return false;
        if (!txStartTime.equals(metadata.txStartTime)) return false;
        if (!txCommitTime.equals(metadata.txCommitTime)) return false;
        return Objects.equals(additionalEntries, metadata.additionalEntries);
    }

    @Override
    public int hashCode() {
        int result = executingUser != null ? executingUser.hashCode() : 0;
        result = 31 * result + (connectionClient != null ? connectionClient.hashCode() : 0);
        result = 31 * result + (authenticatedUser != null ? authenticatedUser.hashCode() : 0);
        result = 31 * result + captureMode.hashCode();
        result = 31 * result + serverId.hashCode();
        result = 31 * result + connectionType.hashCode();
        result = 31 * result + (connectionServer != null ? connectionServer.hashCode() : 0);
        result = 31 * result + txStartTime.hashCode();
        result = 31 * result + txCommitTime.hashCode();
        result = 31 * result + (additionalEntries != null ? additionalEntries.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                "Metadata{authenticatedUser=%s, executingUser=%s, serverId=%s, captureMode=%s, connectionType=%s, connectionClient=%s, connectionServer=%s, txStartTime=%s, txCommitTime=%s, additionalEntries=%s}",
                authenticatedUser,
                executingUser,
                serverId,
                captureMode,
                connectionType,
                connectionClient,
                connectionServer,
                txStartTime,
                txCommitTime,
                additionalEntries);
    }

    public static Metadata fromMap(Map<?, ?> map) {
        var cypherMap = ModelUtils.checkedMap(Objects.requireNonNull(map), String.class, Object.class);

        var authenticatedUser = MapUtils.getString(cypherMap, AUTHENTICATED_USER);
        var executingUser = MapUtils.getString(cypherMap, EXECUTING_USER);
        var captureMode = CaptureMode.valueOf(MapUtils.getString(cypherMap, CAPTURE_MODE));
        var serverId = MapUtils.getString(cypherMap, SERVER_ID);
        var connectionType = MapUtils.getString(cypherMap, CONNECTION_TYPE);
        var connectionClient = MapUtils.getString(cypherMap, CONNECTION_CLIENT);
        var connectionServer = MapUtils.getString(cypherMap, CONNECTION_SERVER);
        var txStartTime = ModelUtils.getZonedDateTime(cypherMap, TX_START_TIME);
        var txCommitTime = ModelUtils.getZonedDateTime(cypherMap, TX_COMMIT_TIME);
        var unknownEntries = cypherMap.entrySet().stream()
                .filter(e -> !KNOWN_KEYS.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new Metadata(
                authenticatedUser,
                executingUser,
                serverId,
                captureMode,
                connectionType,
                connectionClient,
                connectionServer,
                txStartTime,
                txCommitTime,
                unknownEntries);
    }
}
