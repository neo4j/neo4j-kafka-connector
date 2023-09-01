package org.neo4j.cdc.client.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class RelationshipEvent extends Event {

	private final Node start;
	private final Node end;
	private final String type;
	private final Map<String, Object> key;
	private final List<String> labels;

	@JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
	public RelationshipEvent(@JsonProperty("start") Node start,
													 @JsonProperty("end") Node end,
													 @JsonProperty("type") String type,
													 @JsonProperty("key") Map<String, Object> key,
													 @JsonProperty("labels") List<String> labels,
													 @JsonProperty("elementId") String elementId,
													 @JsonProperty("eventType") String eventType,
													 @JsonProperty("state") State state,
													 @JsonProperty("operation") String operation) {
		super(elementId, eventType, state, operation);
		this.start = start;
		this.end = end;
		this.type = type;
		this.key = key;
		this.labels = labels;
	}

	public Node getStart() {
		return this.start;
	}

	public Node getEnd() {
		return this.end;
	}

	public String getType() {
		return this.type;
	}

	public Map<String, Object> getKey() {
		return this.key;
	}

	public List<String> getLabels() {
		return this.labels;
	}
}
