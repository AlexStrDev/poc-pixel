package io.github.axonkafka.serializer;

import io.github.axonkafka.annotation.RoutingKey;
import io.github.axonkafka.serializer.model.SerializedCommand;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.MetaData;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Serializador genérico de comandos de Axon a JSON para Kafka.
 */
@Slf4j
public class CommandSerializer {

    private final ObjectMapper objectMapper;

    public CommandSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public CommandSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Serializa un CommandMessage a JSON para Kafka
     */
    public String serialize(CommandMessage<?> commandMessage, String routingKey) {
        try {
            String messageId = commandMessage.getIdentifier();
            String commandName = commandMessage.getPayloadType().getName();
            long timestamp = System.currentTimeMillis();
            
            // Serializar payload
            Object payload = commandMessage.getPayload();
            String payloadJson = objectMapper.writeValueAsString(payload);
            
            SerializedCommand.SerializedObject serializedPayload = new SerializedCommand.SerializedObject();
            serializedPayload.setType(commandName);
            serializedPayload.setRevision("1.0");
            serializedPayload.setData(payloadJson);
            
            // Convertir MetaData
            Map<String, Object> metaDataMap = new HashMap<>();
            commandMessage.getMetaData().forEach(metaDataMap::put);
            
            // Crear comando serializado
            SerializedCommand serializedCommand = new SerializedCommand();
            serializedCommand.setMessageIdentifier(messageId);
            serializedCommand.setCommandName(commandName);
            serializedCommand.setTimestamp(timestamp);
            serializedCommand.setPayload(serializedPayload);
            serializedCommand.setMetaData(metaDataMap);
            serializedCommand.setRoutingKey(routingKey);
            
            String json = objectMapper.writeValueAsString(serializedCommand);
            log.debug("Comando serializado: {} con routingKey: {}", commandName, routingKey);
            
            return json;
            
        } catch (Exception e) {
            log.error("Error serializando comando: {}", commandMessage, e);
            throw new RuntimeException("Error serializando comando", e);
        }
    }

    /**
     * Deserializa un JSON de Kafka a CommandMessage
     */
    public CommandMessage<?> deserialize(String json) {
        try {
            log.debug("Deserializando comando desde JSON");
            
            SerializedCommand serializedCommand = objectMapper.readValue(json, SerializedCommand.class);
            
            // Deserializar payload
            Class<?> payloadType = Class.forName(serializedCommand.getCommandName());
            Object payload = objectMapper.readValue(
                    serializedCommand.getPayload().getData(),
                    payloadType
            );
            
            // Combinar todos los metadatos
            Map<String, Object> allMetaData = new HashMap<>(serializedCommand.getMetaData());
            allMetaData.put("messageIdentifier", serializedCommand.getMessageIdentifier());
            
            MetaData metaData = MetaData.from(allMetaData);
            
            log.debug("✅ Comando deserializado: {}", payloadType.getSimpleName());
            
            return new GenericCommandMessage<>(payload, metaData);
            
        } catch (Exception e) {
            log.error("Error deserializando comando: {}", json, e);
            throw new RuntimeException("Error deserializando comando", e);
        }
    }

    /**
     * Extrae el routing key de un CommandMessage.
     * 
     * PRIORIDAD:
     * 1. Método anotado con @RoutingKey
     * 2. Campo anotado con @TargetAggregateIdentifier
     * 3. UUID aleatorio
     */
    public String extractRoutingKey(CommandMessage<?> commandMessage) {
        try {
            Object payload = commandMessage.getPayload();
            Class<?> payloadClass = payload.getClass();
            
            // 1. Buscar método con @RoutingKey (PRIORIDAD)
            for (Method method : payloadClass.getDeclaredMethods()) {
                if (method.isAnnotationPresent(RoutingKey.class)) {
                    method.setAccessible(true);
                    Object value = method.invoke(payload);
                    
                    if (value != null) {
                        String routingKey = value.toString();
                        log.debug("✅ RoutingKey extraído de @RoutingKey: {}", routingKey);
                        return routingKey;
                    }
                }
            }
            
            // 2. Buscar método anotado con @TargetAggregateIdentifier
            for (Method method : payloadClass.getDeclaredMethods()) {
                if (method.isAnnotationPresent(org.axonframework.modelling.command.TargetAggregateIdentifier.class)) {
                    method.setAccessible(true);
                    Object value = method.invoke(payload);
                    
                    if (value != null) {
                        String routingKey = value.toString();
                        log.debug("✅ RoutingKey extraído de método @TargetAggregateIdentifier: {}", routingKey);
                        return routingKey;
                    }
                }
            }
            
            // 3. Buscar campo con @TargetAggregateIdentifier (fallback)
            var fields = payloadClass.getDeclaredFields();
            for (var field : fields) {
                if (field.isAnnotationPresent(org.axonframework.modelling.command.TargetAggregateIdentifier.class)) {
                    field.setAccessible(true);
                    Object value = field.get(payload);
                    
                    if (value != null) {
                        String routingKey = value.toString();
                        log.debug("✅ RoutingKey extraído de campo @TargetAggregateIdentifier: {}", routingKey);
                        return routingKey;
                    }
                }
            }
            
            // 4. UUID aleatorio como último recurso
            String randomKey = UUID.randomUUID().toString();
            log.debug("⚠️ No se encontró routing key, usando UUID: {}", randomKey);
            return randomKey;
            
        } catch (Exception e) {
            log.warn("Error extrayendo routingKey, usando UUID aleatorio", e);
            return UUID.randomUUID().toString();
        }
    }
}