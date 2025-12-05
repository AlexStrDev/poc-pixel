package com.axonkafka.starter.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class SerializedCommand {

    @JsonProperty("messageIdentifier")
    private String messageIdentifier;

    @JsonProperty("commandName")
    private String commandName;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("payload")
    private SerializedObject payload;

    @JsonProperty("metaData")
    private Map<String, Object> metaData = new HashMap<>();

    @JsonProperty("routingKey")
    private String routingKey;

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