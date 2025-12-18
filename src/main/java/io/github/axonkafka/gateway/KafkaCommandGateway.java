package io.github.axonkafka.gateway;

import io.github.axonkafka.handler.CommandReplyHandler;
import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.CommandSerializer;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.GenericCommandResultMessage;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.common.Registration;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * CommandGateway genérico con Kafka y request-reply pattern.
 */
@Slf4j
public class KafkaCommandGateway implements CommandGateway {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final CommandSerializer commandSerializer;
    private final CommandReplyHandler replyHandler;
    private final AxonKafkaProperties properties;

    private long sentCommands = 0;
    private long failedSends = 0;

    public KafkaCommandGateway(
            KafkaTemplate<String, String> kafkaTemplate,
            CommandSerializer commandSerializer,
            CommandReplyHandler replyHandler,
            AxonKafkaProperties properties) {
        
        this.kafkaTemplate = kafkaTemplate;
        this.commandSerializer = commandSerializer;
        this.replyHandler = replyHandler;
        this.properties = properties;
    }

    @Override
    public <C, R> void send(C command, CommandCallback<? super C, ? super R> callback) {
        try {
            log.info("📤 Enviando comando: {} al tópico: {}", 
                command.getClass().getSimpleName(), 
                properties.getCommand().getTopic());
            
            String correlationId = UUID.randomUUID().toString();
            
            @SuppressWarnings("unchecked")
            CommandMessage<C> commandMessage = (CommandMessage<C>) GenericCommandMessage.asCommandMessage(command)
                .andMetaData(Map.of("correlationId", correlationId));
            
            String routingKey = commandSerializer.extractRoutingKey(commandMessage);
            String serializedCommand = commandSerializer.serialize(commandMessage, routingKey);
            
            CompletableFuture<CommandReplyHandler.CommandResult> replyFuture = 
                replyHandler.registerPendingCommand(correlationId);
            
            CompletableFuture<SendResult<String, String>> sendFuture = 
                    kafkaTemplate.send(properties.getCommand().getTopic(), routingKey, serializedCommand);
            
            sendFuture.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("❌ Error enviando comando a Kafka", throwable);
                    failedSends++;
                    
                    if (callback != null) {
                        callback.onResult(commandMessage, 
                                GenericCommandResultMessage.asCommandResultMessage(throwable));
                    }
                    
                    replyFuture.completeExceptionally(throwable);
                } else {
                    sentCommands++;
                    log.info("✅ Comando enviado - Offset: {}, CorrelationId: {}", 
                            result.getRecordMetadata().offset(),
                            correlationId);
                    
                    if (callback != null) {
                        replyFuture.thenAccept(commandResult -> {
                            if (commandResult.isSuccess()) {
                                @SuppressWarnings("unchecked")
                                R commandPayload = (R) commandResult.getResult();
                                callback.onResult(commandMessage, 
                                    GenericCommandResultMessage.asCommandResultMessage(commandPayload));
                            } else {
                                callback.onResult(commandMessage, 
                                    GenericCommandResultMessage.asCommandResultMessage(
                                        new CommandExecutionException(commandResult.getErrorMessage())));
                            }
                        }).exceptionally(ex -> {
                            callback.onResult(commandMessage, 
                                GenericCommandResultMessage.asCommandResultMessage(ex));
                            return null;
                        });
                    }
                }
            });
            
        } catch (Exception e) {
            log.error("💥 Error enviando comando", e);
            failedSends++;
            
            if (callback != null) {
                @SuppressWarnings("unchecked")
                CommandMessage<C> commandMessage = (CommandMessage<C>) GenericCommandMessage.asCommandMessage(command);
                callback.onResult(commandMessage, GenericCommandResultMessage.asCommandResultMessage(e));
            }
        }
    }

    @Override
    public <R> R sendAndWait(Object command) {
        return sendAndWait(command, properties.getCommand().getTimeoutSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public <R> R sendAndWait(Object command, long timeout, TimeUnit unit) {
        CompletableFuture<R> result = new CompletableFuture<>();
        
        send(command, new CommandCallback<Object, R>() {
            @Override
            public void onResult(CommandMessage<? extends Object> commandMessage, 
                               org.axonframework.commandhandling.CommandResultMessage<? extends R> commandResultMessage) {
                if (commandResultMessage.isExceptional()) {
                    result.completeExceptionally(commandResultMessage.exceptionResult());
                } else {
                    result.complete(commandResultMessage.getPayload());
                }
            }
        });
        
        try {
            return result.get(timeout, unit);
        } catch (Exception e) {
            throw new RuntimeException("Error esperando respuesta del comando", e);
        }
    }

    @Override
    public <R> CompletableFuture<R> send(Object command) {
        CompletableFuture<R> future = new CompletableFuture<>();
        
        send(command, new CommandCallback<Object, R>() {
            @Override
            public void onResult(CommandMessage<? extends Object> commandMessage, 
                               org.axonframework.commandhandling.CommandResultMessage<? extends R> commandResultMessage) {
                if (commandResultMessage.isExceptional()) {
                    future.completeExceptionally(commandResultMessage.exceptionResult());
                } else {
                    future.complete(commandResultMessage.getPayload());
                }
            }
        });
        
        return future;
    }

    @Override
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super CommandMessage<?>> dispatchInterceptor) {
        log.warn("registerDispatchInterceptor no está completamente implementado");
        return () -> true;
    }

    public Map<String, Object> getMetrics() {
        return Map.of(
            "sentCommands", sentCommands,
            "failedSends", failedSends,
            "successRate", sentCommands > 0 ? 
                (sentCommands - failedSends) * 100.0 / sentCommands : 0
        );
    }

    public static class CommandExecutionException extends RuntimeException {
        public CommandExecutionException(String message) {
            super(message);
        }
    }
}