package io.github.axonkafka.serializer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Modelo genérico para representar un evento serializado en Kafka.
 */
@Data
public class SerializedEvent {
    
    @JsonProperty("eventIdentifier")
    private String eventIdentifier;
    
    @JsonProperty("aggregateIdentifier")
    private String aggregateIdentifier;
    
    @JsonProperty("sequenceNumber")
    private Long sequenceNumber;
    
    @JsonProperty("eventType")
    private String eventType;
    
    @JsonProperty("timestamp")
    private long timestamp;
    
    @JsonProperty("payload")
    private SerializedObject payload;
    
    @JsonProperty("metaData")
    private Map<String, Object> metaData = new HashMap<>();
    
    @Data
    public static class SerializedObject {
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("revision")
        private String revision;
        
        @JsonProperty("data")
        private String data;
    }
}