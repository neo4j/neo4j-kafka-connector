package org.neo4j.cdc.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.ZonedDateTime;

public class Metadata {

	private final String executingUser;
	private final String connectionClient;
	private final String authenticatedUser;
	private final CaptureMode captureMode;
	private final String serverId;
	private final String connectionType;
	private final String connectionServer;
	private final ZonedDateTime txStartTime;
	private final ZonedDateTime txCommitTime;

	@JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
	public Metadata(@JsonProperty("executingUser") String executingUser,
									@JsonProperty("connectionClient") String connectionClient,
									@JsonProperty("authenticatedUser") String authenticatedUser,
									@JsonProperty("captureMode") CaptureMode captureMode,
									@JsonProperty("serverId") String serverId,
									@JsonProperty("connectionType") String connectionType,
									@JsonProperty("connectionServer") String connectionServer,
									@JsonProperty("txStartTime") @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX") ZonedDateTime txStartTime,
									@JsonProperty("txCommitTime") @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX") ZonedDateTime txCommitTime) {
		this.executingUser = executingUser;
		this.connectionClient = connectionClient;
		this.authenticatedUser = authenticatedUser;
		this.captureMode = captureMode;
		this.serverId = serverId;
		this.connectionType = connectionType;
		this.connectionServer = connectionServer;
		this.txStartTime = txStartTime;
		this.txCommitTime = txCommitTime;
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

}
