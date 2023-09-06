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
package org.neo4j.cdc.client;

import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.cdc.client.model.Event;
import org.neo4j.cdc.client.model.Metadata;

/**
 * @author Gerrit Meier
 */
public final class ResultMapper {
    private static final String ID_FIELD = "id";
    private static final String TX_ID_FIELD = "txId";
    private static final String SEQ_FIELD = "seq";
    private static final String METADATA_FIELD = "metadata";
    private static final String EVENT_FIELD = "event";

    private ResultMapper() {}

    public static ChangeIdentifier parseChangeIdentifier(Map<String, Object> message) {
        return new ChangeIdentifier((String) message.get(ID_FIELD));
    }

    @SuppressWarnings("unchecked")
    public static ChangeEvent parseChangeEvent(Map<String, Object> message) {
        ChangeIdentifier changeIdentifier = new ChangeIdentifier(MapUtils.getString(message, ID_FIELD));
        Long txId = MapUtils.getLong(message, TX_ID_FIELD);
        int seq = MapUtils.getIntValue(message, SEQ_FIELD);
        Metadata metadata = new Metadata((Map<String, Object>) MapUtils.getMap(message, "metadata"));
        Event event = Event.create((Map<String, Object>) MapUtils.getMap(message, "event"));

        return new ChangeEvent(changeIdentifier, txId, seq, metadata, event);
    }
}
