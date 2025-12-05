package com.axonkafka.starter.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class CommandDeduplicationService {

    private final Map<String, ProcessedCommand> processedCommands;
    private final ScheduledExecutorService cleanupScheduler;
    private final long retentionMinutes;

    public CommandDeduplicationService() {
        this(60);
    }

    public CommandDeduplicationService(long retentionMinutes) {
        this.processedCommands = new ConcurrentHashMap<>();
        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor();
        this.retentionMinutes = retentionMinutes;
        startCleanupTask();
    }

    public boolean markAsProcessed(String messageId) {
        ProcessedCommand existing = processedCommands.putIfAbsent(
                messageId,
                new ProcessedCommand(messageId, System.currentTimeMillis())
        );

        if (existing == null) {
            log.debug("Comando marcado como procesado: {}", messageId);
            return true;
        } else {
            log.warn("⚠️ Comando duplicado detectado y bloqueado: {}", messageId);
            return false;
        }
    }

    public boolean wasProcessed(String messageId) {
        return processedCommands.containsKey(messageId);
    }

    public void remove(String messageId) {
        processedCommands.remove(messageId);
    }

    public Map<String, Object> getStats() {
        return Map.of(
                "cachedCommands", processedCommands.size(),
                "retentionMinutes", retentionMinutes
        );
    }

    private void startCleanupTask() {
        cleanupScheduler.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                long expirationTime = TimeUnit.MINUTES.toMillis(retentionMinutes);

                int initialSize = processedCommands.size();

                processedCommands.entrySet().removeIf(entry ->
                        (now - entry.getValue().getTimestamp()) > expirationTime
                );

                int removed = initialSize - processedCommands.size();
                if (removed > 0) {
                    log.info("Limpieza de cache: {} comandos expirados removidos, {} restantes",
                            removed, processedCommands.size());
                }

            } catch (Exception e) {
                log.error("Error en limpieza de cache", e);
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    public void shutdown() {
        cleanupScheduler.shutdown();
    }

    private static class ProcessedCommand {
        private final String messageId;
        private final long timestamp;

        public ProcessedCommand(String messageId, long timestamp) {
            this.messageId = messageId;
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}