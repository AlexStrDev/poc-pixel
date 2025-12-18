package io.github.axonkafka.bus;

import io.github.axonkafka.cache.CommandDeduplicationService;
import io.github.axonkafka.handler.CommandReplyHandler;
import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.CommandSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * CommandBus genérico con Kafka que incluye:
 * - Idempotencia
 * - DLQ para comandos fallidos
 * - Request-Reply pattern
 */
@Slf4j
public class KafkaCommandBus {

    private final CommandBus localCommandBus;
    private final CommandSerializer commandSerializer;
    private final CommandDeduplicationService deduplicationService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Map<String, List<MessageHandler<? super CommandMessage<?>>>> handlers;
    private final List<MessageHandlerInterceptor<? super CommandMessage<?>>> handlerInterceptors;
    private final AxonKafkaProperties properties;

    private long processedCommands = 0;
    private long failedCommands = 0;
    private long duplicateCommands = 0;

    public KafkaCommandBus(
            CommandBus localCommandBus,
            CommandSerializer commandSerializer,
            CommandDeduplicationService deduplicationService,
            KafkaTemplate<String, String> kafkaTemplate,
            AxonKafkaProperties properties) {
        
        this.localCommandBus = localCommandBus;
        this.commandSerializer = commandSerializer;
        this.deduplicationService = deduplicationService;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.handlers = new ConcurrentHashMap<>();
        this.handlerInterceptors = new CopyOnWriteArrayList<>();
    }

    public org.axonframework.common.Registration subscribe(
            @Nonnull String commandName,
            @Nonnull MessageHandler<? super CommandMessage<?>> handler) {
        
        log.info("Registrando handler para comando: {}", commandName);
        
        handlers.computeIfAbsent(commandName, k -> new CopyOnWriteArrayList<>()).add(handler);
        
        org.axonframework.common.Registration localRegistration = 
            localCommandBus.subscribe(commandName, handler);
        
        return () -> {
            List<MessageHandler<? super CommandMessage<?>>> commandHandlers = handlers.get(commandName);
            if (commandHandlers != null) {
                commandHandlers.remove(handler);
                if (commandHandlers.isEmpty()) {
                    handlers.remove(commandName);
                }
            }
            return localRegistration.cancel();
        };
    }

    @KafkaListener(
            topics = "${axon.kafka.command.topic}",
            groupId = "${axon.kafka.command.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeCommand(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String messageId = null;
        String correlationId = null;
        
        try {
            log.info("📨 Comando recibido - Key: {}", record.key());
            
            String commandJson = record.value();
            CommandMessage<?> commandMessage = commandSerializer.deserialize(commandJson);
            
            messageId = extractMessageId(commandMessage);
            correlationId = extractCorrelationId(commandMessage);
            
            // Idempotencia
            if (!deduplicationService.markAsProcessed(messageId)) {
                log.warn("⚠️ Comando duplicado bloqueado: {}", messageId);
                duplicateCommands++;
                acknowledgment.acknowledge();
                
                if (correlationId != null) {
                    sendReply(correlationId, true, "Comando duplicado");
                }
                return;
            }
            
            log.debug("Comando deserializado: {}", commandMessage.getPayloadType().getSimpleName());
            
            processCommandWithReply(commandMessage, correlationId);
            
            processedCommands++;
            acknowledgment.acknowledge();
            
        } catch (BusinessException e) {
            log.error("❌ Error de negocio: {}", e.getMessage());
            failedCommands++;
            
            sendToDLQ(record, e.getMessage());
            sendReply(correlationId, false, "Error de negocio: " + e.getMessage());
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("💥 Error técnico (se reintentará): {}", e.getMessage());
            failedCommands++;
            
            sendReply(correlationId, false, "Error técnico: " + e.getMessage());
        }
    }

    private void processCommandWithReply(CommandMessage<?> commandMessage, String correlationId) {
        try {
            log.info("⚙️ Procesando comando: {}", commandMessage.getPayloadType().getSimpleName());
            
            localCommandBus.dispatch(commandMessage, new CommandCallback<Object, Object>() {
                @Override
                public void onResult(@Nonnull CommandMessage<?> commandMessage, 
                                     @Nonnull org.axonframework.commandhandling.CommandResultMessage<?> result) {
                    if (result.isExceptional()) {
                        Throwable exception = result.exceptionResult();
                        log.error("❌ Error procesando comando: {}", exception.getMessage());
                        
                        sendReply(correlationId, false, exception.getMessage());
                        
                        if (exception instanceof BusinessException) {
                            throw (BusinessException) exception;
                        }
                    } else {
                        log.info("✅ Comando procesado exitosamente");
                        
                        String resultStr = result.getPayload() != null ? 
                            result.getPayload().toString() : "OK";
                        sendReply(correlationId, true, resultStr);
                    }
                }
            });
            
        } catch (Exception e) {
            log.error("Error despachando comando", e);
            throw e;
        }
    }

    private void sendToDLQ(ConsumerRecord<String, String> record, String errorMessage) {
        try {
            String dlqMessage = String.format(
                "{\"originalKey\":\"%s\",\"originalValue\":%s,\"error\":\"%s\",\"timestamp\":%d}",
                record.key(), record.value(), errorMessage, System.currentTimeMillis()
            );
            
            String dlqTopic = properties.getCommand().getDlqTopic();
            kafkaTemplate.send(dlqTopic, record.key(), dlqMessage);
            log.info("📮 Comando enviado a DLQ: {}", dlqTopic);
            
        } catch (Exception e) {
            log.error("Error enviando comando a DLQ", e);
        }
    }

    private void sendReply(String correlationId, boolean success, String message) {
        if (correlationId == null) {
            return;
        }
        
        try {
            CommandReplyHandler.CommandResult result = success ?
                CommandReplyHandler.CommandResult.success(correlationId, message) :
                CommandReplyHandler.CommandResult.error(correlationId, message);
            
            String resultJson = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(result);
            
            String replyTopic = properties.getCommand().getReplyTopic();
            
            kafkaTemplate.send(replyTopic, correlationId, resultJson);
            log.debug("📤 Respuesta enviada a: {}", replyTopic);
            
        } catch (Exception e) {
            log.error("💥 Error enviando respuesta", e);
        }
    }

    private String extractMessageId(CommandMessage<?> commandMessage) {
        Object messageId = commandMessage.getMetaData().get("messageIdentifier");
        return messageId != null ? messageId.toString() : commandMessage.getIdentifier();
    }

    private String extractCorrelationId(CommandMessage<?> commandMessage) {
        Object correlationId = commandMessage.getMetaData().get("correlationId");
        return correlationId != null ? correlationId.toString() : null;
    }

    public Map<String, Object> getMetrics() {
        return Map.of(
            "processedCommands", processedCommands,
            "failedCommands", failedCommands,
            "duplicateCommands", duplicateCommands,
            "successRate", processedCommands > 0 ? 
                (processedCommands - failedCommands) * 100.0 / processedCommands : 0
        );
    }

    public org.axonframework.common.Registration registerHandlerInterceptor(
            @Nonnull MessageHandlerInterceptor<? super CommandMessage<?>> handlerInterceptor) {
        handlerInterceptors.add(handlerInterceptor);
        return () -> handlerInterceptors.remove(handlerInterceptor);
    }

    public org.axonframework.common.Registration registerDispatchInterceptor(
            @Nonnull org.axonframework.messaging.MessageDispatchInterceptor<? super CommandMessage<?>> dispatchInterceptor) {
        return localCommandBus.registerDispatchInterceptor(dispatchInterceptor);
    }

    public static class BusinessException extends RuntimeException {
        public BusinessException(String message) {
            super(message);
        }
    }
}