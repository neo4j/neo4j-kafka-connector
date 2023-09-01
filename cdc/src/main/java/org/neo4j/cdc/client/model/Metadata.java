package org.neo4j.cdc.client.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.ZonedDateTime;

public class Metadata {

    private String executingUser;
    private String connectionClient;
    private String authenticatedUser;
    private CaptureMode captureMode;
    private String serverId;
    private String connectionType;
    private String connectionServer;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX")
    private ZonedDateTime txStartTime;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX")
    private ZonedDateTime txCommitTime;

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

}
