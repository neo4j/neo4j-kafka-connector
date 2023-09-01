package org.neo4j.cdc.client;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.cdc.client.model.CaptureMode;
import org.neo4j.cdc.client.model.ChangeEvent;
import org.neo4j.cdc.client.model.ChangeIdentifier;
import org.neo4j.cdc.client.model.Event;
import org.neo4j.cdc.client.model.Metadata;
import org.neo4j.cdc.client.model.Node;
import org.neo4j.cdc.client.model.NodeEvent;
import org.neo4j.cdc.client.model.RelationshipEvent;
import org.neo4j.cdc.client.model.State;

/**
 * @author Gerrit Meier
 */
public class ResultMapperTest {

    @Test
    void shouldParseChangeIdentifier() {
        String changeIdentifierValue = "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA";
        Map<String, Object> message = new HashMap<>();
        message.put("id", changeIdentifierValue);
        ChangeIdentifier result = ResultMapper.parseChangeIdentifier(message);
        assertEquals(result.getId(), changeIdentifierValue);
    }

    @Test
    void shouldParseCompleteChangeNodeEventRecord() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("executingUser", "neo4j");
        metadata.put("connectionClient", "172.17.0.1:44484");
        metadata.put("authenticatedUser", "neo4j");
        metadata.put("captureMode", "FULL");
        metadata.put("serverId", "60b75468");
        metadata.put("connectionType", "bolt");
        metadata.put("connectionServer", "172.17.0.2:7687");
        metadata.put("txStartTime", "2023-08-17T09:14:35.636000000Z");
        metadata.put("txCommitTime", "2023-08-17T09:14:35.666000000Z");

        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "someone");
        properties.put("real_name", "Some real name");
        Map<String, Object> afterState = new HashMap<>();
        afterState.put("properties", properties);
        afterState.put("labels", Collections.singletonList("User"));
        Map<String, Object> state = new HashMap<>();
        state.put("before", null);
        state.put("after", afterState);

        Map<String, Object> event = new HashMap<>();
        event.put("elementId", "4:5bd54b2f-b8b3-4c9a-89ad-f54979871f3f:0");
        event.put("keys", emptyMap());
        event.put("eventType", "n");
        event.put("state", state);
        event.put("operation", "c");
        event.put("labels", Collections.singletonList("User"));

        Map<String, Object> message = new HashMap<>();
        message.put("id", "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA");
        message.put("txId", 3L);
        message.put("seq", 1L);
        message.put("metadata", metadata);
        message.put("event", event);

        ChangeEvent changeEvent = ResultMapper.parseChangeEvent(message);
        assertEquals(changeEvent.getId().getId(), "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA");
        assertEquals(changeEvent.getTxId(), 3L);
        assertEquals(changeEvent.getSeq(), 1);

        checkMetadata(changeEvent.getMetadata());
        Event changeEventEvent = changeEvent.getEvent();
        assertInstanceOf(NodeEvent.class, changeEventEvent);
        NodeEvent nodeEvent = (NodeEvent) changeEventEvent;
        assertEquals(nodeEvent.getElementId(), "4:5bd54b2f-b8b3-4c9a-89ad-f54979871f3f:0");
        assertEquals(nodeEvent.getKeys(), emptyMap());
        assertEquals(nodeEvent.getEventType(), "n");
        assertEquals(nodeEvent.getLabels().get(0), "User");
        assertEquals(nodeEvent.getOperation(), "c");
        State changeEventState = nodeEvent.getState();
        assertNotNull(changeEventState);
        assertNull(changeEventState.getBefore());
        assertEquals(((Map<String, Object>) changeEventState.getAfter().get("properties")).get("name"), "someone");
        assertEquals(
                ((Map<String, Object>) changeEventState.getAfter().get("properties")).get("real_name"),
                "Some real name");
        assertEquals(((List<String>) changeEventState.getAfter().get("labels")).get(0), "User");
    }

    @Test
    void shouldParseCompleteChangeRelationshipEventRecord() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("executingUser", "neo4j");
        metadata.put("connectionClient", "172.17.0.1:44484");
        metadata.put("authenticatedUser", "neo4j");
        metadata.put("captureMode", "FULL");
        metadata.put("serverId", "60b75468");
        metadata.put("connectionType", "bolt");
        metadata.put("connectionServer", "172.17.0.2:7687");
        metadata.put("txStartTime", "2023-08-17T09:14:35.636000000Z");
        metadata.put("txCommitTime", "2023-08-17T09:14:35.666000000Z");

        Map<String, Object> properties = new HashMap<>();
        properties.put("roles", "Jack Swigert");
        Map<String, Object> afterState = new HashMap<>();
        afterState.put("properties", properties);
        afterState.put("labels", Collections.singletonList("User"));
        Map<String, Object> state = new HashMap<>();
        state.put("before", null);
        state.put("after", afterState);

        Map<String, Object> start = new HashMap<>();
        start.put("elementId", "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0");
        start.put("labels", Collections.singletonList("PERSON"));
        start.put("keys", Collections.emptyMap());

        Map<String, Object> end = new HashMap<>();
        end.put("elementId", "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:1");
        end.put("labels", Collections.singletonList("MOVIE"));
        end.put("keys", Collections.emptyMap());

        Map<String, Object> event = new HashMap<>();
        event.put("elementId", "5:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0");
        event.put("start", start);
        event.put("end", end);
        event.put("key", emptyMap());
        event.put("eventType", "r");
        event.put("state", state);
        event.put("operation", "c");
        event.put("type", "ACTED_IN");

        Map<String, Object> message = new HashMap<>();
        message.put("id", "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA");
        message.put("txId", 4L);
        message.put("seq", 2L);
        message.put("metadata", metadata);
        message.put("event", event);

        ChangeEvent changeEvent = ResultMapper.parseChangeEvent(message);
        assertEquals(changeEvent.getId().getId(), "AlvVSy-4s0yaia31SXmHHz8AAAAAAAAACgAAAAAAAAAA");
        assertEquals(changeEvent.getTxId(), 4L);
        assertEquals(changeEvent.getSeq(), 2);
        checkMetadata(changeEvent.getMetadata());
        Event changeEventEvent = changeEvent.getEvent();
        assertInstanceOf(RelationshipEvent.class, changeEventEvent);
        RelationshipEvent relationshipEvent = (RelationshipEvent) changeEventEvent;
        assertEquals(relationshipEvent.getElementId(), "5:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0");
        assertEquals(relationshipEvent.getType(), "ACTED_IN");
        assertEquals(relationshipEvent.getOperation(), "c");
        assertEquals(relationshipEvent.getEventType(), "r");
        assertNull(relationshipEvent.getState().getBefore());
        Map<String, Object> stateAfter = relationshipEvent.getState().getAfter();
        assertEquals(((Map<String, Object>) stateAfter.get("properties")).get("roles"), "Jack Swigert");

        Node startElement = relationshipEvent.getStart();
        assertEquals(startElement.getElementId(), "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:0");
        assertEquals(startElement.getKeys(), emptyMap());
        assertEquals(startElement.getLabels().get(0), "PERSON");

        Node endElement = relationshipEvent.getEnd();
        assertEquals(endElement.getElementId(), "4:6a4af4ff-da3a-49e7-ae71-2c0ac3c1fc1a:1");
        assertEquals(endElement.getKeys(), emptyMap());
        assertEquals(endElement.getLabels().get(0), "MOVIE");
    }

    private void checkMetadata(Metadata metadata) {
        assertEquals(metadata.getExecutingUser(), "neo4j");
        assertEquals(metadata.getConnectionClient(), "172.17.0.1:44484");
        assertEquals(metadata.getAuthenticatedUser(), "neo4j");
        assertEquals(metadata.getCaptureMode(), CaptureMode.FULL);
        assertEquals(metadata.getServerId(), "60b75468");
        assertEquals(metadata.getConnectionType(), "bolt");
        assertEquals(metadata.getConnectionServer(), "172.17.0.2:7687");
        assertEquals(
                metadata.getTxStartTime(),
                ZonedDateTime.parse(
                        "2023-08-17T09:14:35.636000000Z",
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX")));
        assertEquals(
                metadata.getTxCommitTime(),
                ZonedDateTime.parse(
                        "2023-08-17T09:14:35.666000000Z",
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX")));
    }
}
