package org.neo4j.cdc.client.model;

public class ChangeEvent {

    private final ChangeIdentifier id;
    private final Long txId;
    private final Integer seq;
    private final Metadata metadata;
    private final Event event;

    public ChangeEvent(ChangeIdentifier id, Long txId, Integer seq, Metadata metadata, Event event) {
        this.id = id;
        this.txId = txId;
        this.seq = seq;
        this.metadata = metadata;
        this.event = event;
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
}
