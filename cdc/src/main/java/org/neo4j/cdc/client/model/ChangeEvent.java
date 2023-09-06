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

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class ChangeEvent {

    private final ChangeIdentifier id;
    private final Long txId;
    private final Integer seq;
    private final Metadata metadata;
    private final Event event;

    public ChangeEvent(
            @NotNull ChangeIdentifier id,
            @NotNull Long txId,
            @NotNull Integer seq,
            @NotNull Metadata metadata,
            @NotNull Event event) {
        this.id = Objects.requireNonNull(id);
        this.txId = Objects.requireNonNull(txId);
        this.seq = Objects.requireNonNull(seq);
        this.metadata = Objects.requireNonNull(metadata);
        this.event = Objects.requireNonNull(event);
    }

    public ChangeIdentifier getId() {
        return this.id;
    }

    public Long getTxId() {
        return this.txId;
    }

    public Integer getSeq() {
        return this.seq;
    }

    public Metadata getMetadata() {
        return this.metadata;
    }

    public Event getEvent() {
        return this.event;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChangeEvent that = (ChangeEvent) o;

        if (!id.equals(that.id)) return false;
        if (!txId.equals(that.txId)) return false;
        if (!seq.equals(that.seq)) return false;
        if (!metadata.equals(that.metadata)) return false;
        return event.equals(that.event);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + txId.hashCode();
        result = 31 * result + seq.hashCode();
        result = 31 * result + metadata.hashCode();
        result = 31 * result + event.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format(
                "ChangeEvent{id=%s, txId=%s, seq=%s, metadata=%s, event=%s}", id, txId, seq, metadata, event);
    }
}
