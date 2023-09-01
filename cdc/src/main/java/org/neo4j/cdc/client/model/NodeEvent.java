package org.neo4j.cdc.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class NodeEvent extends Event {

	private final Map<String, Object> keys;
	private final List<String> labels;

	@JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
	public NodeEvent(@JsonProperty("keys") Map<String, Object> keys,
									 @JsonProperty("labels") List<String> labels,
									 @JsonProperty("elementId") String elementId,
									 @JsonProperty("eventType") String eventType,
									 @JsonProperty("state") State state,
									 @JsonProperty("operation") String operation) {

		super(elementId, eventType, state, operation);
		this.keys = keys;
		this.labels = labels;
	}

	public Map<String, Object> getKeys() {
		return this.keys;
	}

	public List<String> getLabels() {
		return this.labels;
	}
}
