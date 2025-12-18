package io.github.axonkafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.axonkafka.properties.AxonKafkaProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Handler genérico para respuestas de comandos (Request-Reply pattern).
 */
@Slf4j
public class CommandReplyHandler {

    private final ObjectMapper objectMapper;
    private final Map<String, CompletableFuture<CommandResult>> pendingCommands;
    private static final long TIMEOUT_MINUTES = 5;
    private final AxonKafkaProperties properties;

    public CommandReplyHandler(AxonKafkaProperties properties) {
        this.objectMapper = new ObjectMapper();
        this.pendingCommands = new ConcurrentHashMap<>();
        this.properties = properties;
        startCleanupTask();
    }

    public CompletableFuture<CommandResult> registerPendingCommand(String correlationId) {
        CompletableFuture<CommandResult> future = new CompletableFuture<>();
        
        future.orTimeout(TIMEOUT_MINUTES, TimeUnit.MINUTES)
            .exceptionally(throwable -> {
                pendingCommands.remove(correlationId);
                log.warn("Timeout esperando respuesta: {}", correlationId);
                return CommandResult.timeout(correlationId);
            });
        
        pendingCommands.put(correlationId, future);
        log.debug("Comando registrado: {}", correlationId);
        
        return future;
    }

    @KafkaListener(
        topics = "${axon.kafka.command.reply-topic}",
        groupId = "${axon.kafka.command.reply-group-id}",
        containerFactory = "commandReplyKafkaListenerContainerFactory"
    )
    public void handleCommandReply(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📨 Respuesta recibida - Key: {}", record.key());
            
            CommandResult result = objectMapper.readValue(record.value(), CommandResult.class);
            
            log.debug("🔍 Procesando respuesta: {}", result.getCorrelationId());
            
            CompletableFuture<CommandResult> future = pendingCommands.remove(result.getCorrelationId());
            
            if (future != null) {
                if (result.isSuccess()) {
                    future.complete(result);
                    log.info("✅ Comando completado: {}", result.getCorrelationId());
                } else {
                    future.completeExceptionally(
                        new CommandExecutionException(result.getErrorMessage())
                    );
                    log.warn("⚠️ Comando falló: {}", result.getCorrelationId());
                }
            } else {
                log.warn("⚠️ Respuesta para comando no registrado: {}", result.getCorrelationId());
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("💥 Error procesando respuesta: {}", record.value(), e);
            acknowledgment.acknowledge();
        }
    }

    public Map<String, Object> getStats() {
        return Map.of(
            "pendingCommands", pendingCommands.size(),
            "timeoutMinutes", TIMEOUT_MINUTES
        );
    }

    private void startCleanupTask() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                    
                    pendingCommands.entrySet().removeIf(entry -> 
                        entry.getValue().isDone() || entry.getValue().isCompletedExceptionally()
                    );
                    
                    log.debug("Comandos pendientes: {}", pendingCommands.size());
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "command-reply-cleanup").start();
    }

    @Data
    public static class CommandResult {
        private String correlationId;
        private boolean success;
        private String result;
        private String errorMessage;
        private long timestamp;

        public static CommandResult success(String correlationId, String result) {
            CommandResult cr = new CommandResult();
            cr.correlationId = correlationId;
            cr.success = true;
            cr.result = result;
            cr.timestamp = System.currentTimeMillis();
            return cr;
        }

        public static CommandResult error(String correlationId, String errorMessage) {
            CommandResult cr = new CommandResult();
            cr.correlationId = correlationId;
            cr.success = false;
            cr.errorMessage = errorMessage;
            cr.timestamp = System.currentTimeMillis();
            return cr;
        }

        public static CommandResult timeout(String correlationId) {
            return error(correlationId, "Timeout esperando respuesta");
        }
    }

    public static class CommandExecutionException extends RuntimeException {
        public CommandExecutionException(String message) {
            super(message);
        }
    }
}