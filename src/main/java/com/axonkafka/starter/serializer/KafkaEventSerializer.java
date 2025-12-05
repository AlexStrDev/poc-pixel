package com.axonkafka.starter.serializer;

import com.axonkafka.starter.model.SerializedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class KafkaEventSerializer {

    private final ObjectMapper objectMapper;

    public KafkaEventSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public String serialize(DomainEventMessage<?> eventMessage) {
        try {
            String payloadJson = objectMapper.writeValueAsString(eventMessage.getPayload());

            SerializedEvent.SerializedObject payload = new SerializedEvent.SerializedObject();
            payload.setType(eventMessage.getPayloadType().getName());
            payload.setRevision("1.0");
            payload.setData(payloadJson);

            Map<String, Object> metaDataMap = new HashMap<>();
            eventMessage.getMetaData().forEach(metaDataMap::put);

            SerializedEvent serializedEvent = new SerializedEvent();
            serializedEvent.setEventIdentifier(eventMessage.getIdentifier());
            serializedEvent.setAggregateIdentifier(eventMessage.getAggregateIdentifier());
            serializedEvent.setSequenceNumber(eventMessage.getSequenceNumber());
            serializedEvent.setEventType(eventMessage.getPayloadType().getName());
            serializedEvent.setTimestamp(eventMessage.getTimestamp().toEpochMilli());
            serializedEvent.setPayload(payload);
            serializedEvent.setMetaData(metaDataMap);

            return objectMapper.writeValueAsString(serializedEvent);
        } catch (Exception e) {
            log.error("Error serializando evento", e);
            throw new RuntimeException("Error serializando evento", e);
        }
    }

    public DomainEventMessage<?> deserialize(String json) {
        try {
            SerializedEvent serializedEvent = objectMapper.readValue(json, SerializedEvent.class);

            Class<?> payloadType = Class.forName(serializedEvent.getEventType());
            Object payload = objectMapper.readValue(
                    serializedEvent.getPayload().getData(),
                    payloadType
            );

            MetaData metaData = MetaData.from(serializedEvent.getMetaData());

            return new GenericDomainEventMessage<>(
                    serializedEvent.getEventType(),
                    serializedEvent.getAggregateIdentifier(),
                    serializedEvent.getSequenceNumber(),
                    payload,
                    metaData,
                    serializedEvent.getEventIdentifier(),
                    Instant.ofEpochMilli(serializedEvent.getTimestamp())
            );
        } catch (Exception e) {
            log.error("Error deserializando evento: {}", json, e);
            throw new RuntimeException("Error deserializando evento", e);
        }
    }
}