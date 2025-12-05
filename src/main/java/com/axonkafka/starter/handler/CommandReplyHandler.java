package com.axonkafka.starter.handler;

import com.axonkafka.starter.properties.AxonKafkaProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class CommandReplyHandler {

    private final ObjectMapper objectMapper;
    private final Map<String, CompletableFuture<CommandResult>> pendingCommands;
    private static final long TIMEOUT_MINUTES = 5;

    public CommandReplyHandler() {
        this.objectMapper = new ObjectMapper();
        this.pendingCommands = new ConcurrentHashMap<>();
        startCleanupTask();
    }

    public CompletableFuture<CommandResult> registerPendingCommand(String correlationId) {
        CompletableFuture<CommandResult> future = new CompletableFuture<>();

        future.orTimeout(TIMEOUT_MINUTES, TimeUnit.MINUTES)
                .exceptionally(throwable -> {
                    pendingCommands.remove(correlationId);
                    log.warn("Timeout esperando respuesta de comando: {}", correlationId);
                    return CommandResult.timeout(correlationId);
                });

        pendingCommands.put(correlationId, future);
        log.debug("Comando registrado esperando respuesta: {}", correlationId);

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
            log.info("📨 Respuesta recibida del tópico '{}' - Key: {}",
                    record.topic(), record.key());
            log.debug("📨 Valor recibido: {}", record.value());

            CommandResult result = objectMapper.readValue(record.value(), CommandResult.class);

            log.info("🔍 Procesando respuesta para correlationId: {}", result.getCorrelationId());
            log.debug("📊 Comandos pendientes actuales: {}", pendingCommands.keySet());

            CompletableFuture<CommandResult> future = pendingCommands.remove(result.getCorrelationId());

            if (future != null) {
                if (result.isSuccess()) {
                    future.complete(result);
                    log.info("✅ Comando completado exitosamente: {}", result.getCorrelationId());
                } else {
                    future.completeExceptionally(
                            new CommandExecutionException(result.getErrorMessage())
                    );
                    log.warn("⚠️ Comando falló: {} - Error: {}",
                            result.getCorrelationId(), result.getErrorMessage());
                }
            } else {
                log.warn("⚠️ Respuesta recibida para comando no registrado o ya expirado: {}",
                        result.getCorrelationId());
            }

            acknowledgment.acknowledge();
            log.debug("✅ Respuesta confirmada en Kafka");

        } catch (Exception e) {
            log.error("💥 Error procesando respuesta de comando: {}", record.value(), e);
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

                    log.debug("Comandos pendientes después de limpieza: {}", pendingCommands.size());

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
            return error(correlationId, "Timeout esperando respuesta del comando");
        }
    }

    public static class CommandExecutionException extends RuntimeException {
        public CommandExecutionException(String message) {
            super(message);
        }
    }
}