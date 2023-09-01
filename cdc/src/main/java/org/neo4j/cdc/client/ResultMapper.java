package org.neo4j.cdc.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.cdc.client.model.Event;
import org.neo4j.cdc.client.model.Metadata;

import java.util.Map;

/**
 * @author Gerrit Meier
 */
public final class ResultMapper {
	private final static String ID_FIELD = "id";
	private final static String TX_ID_FIELD = "txId";
	private final static String SEQ_FIELD = "seq";
	private final static String METADATA_FIELD = "metadata";
	private final static String EVENT_FIELD = "event";

	private ResultMapper() {

	}

	private final static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

	public static ChangeIdentifier parseChangeIdentifier(Map<String, Object> message) {
		return new ChangeIdentifier((String) message.get(ID_FIELD));
	}

	public static ChangeEvent parseChangeEvent(Map<String, Object> message) {
		ChangeIdentifier changeIdentifier = parseChangeIdentifier(message);
		Long txId = (Long) message.get(TX_ID_FIELD);
		int seq = ((Long) message.get(SEQ_FIELD)).intValue();
		Metadata metadata = objectMapper.convertValue(message.get(METADATA_FIELD), Metadata.class);
		Event event = objectMapper.convertValue(message.get(EVENT_FIELD), Event.class);

		return new ChangeEvent(changeIdentifier, txId, seq, metadata, event);
	}
}
