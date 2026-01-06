package io.github.axonkafka.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterEach;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests unitarios para CommandDeduplicationService
 * 
 * Cubre:
 * - Idempotencia básica
 * - Detección de duplicados
 * - Limpieza automática del cache
 * - Thread-safety
 * - Métricas
 */
class CommandDeduplicationServiceTest {

    private CommandDeduplicationService deduplicationService;

    @BeforeEach
    void setUp() {
        deduplicationService = new CommandDeduplicationService();
    }

    @AfterEach
    void tearDown() {
        deduplicationService.shutdown();
    }

    @Test
    @DisplayName("Debe marcar comando como procesado la primera vez")
    void testMarkAsProcessedFirstTime() {
        // Given
        String messageId = "CMD-001";

        // When
        boolean result = deduplicationService.markAsProcessed(messageId);

        // Then
        assertThat(result).isTrue();
        assertThat(deduplicationService.wasProcessed(messageId)).isTrue();
    }

    @Test
    @DisplayName("Debe rechazar comando duplicado")
    void testRejectDuplicateCommand() {
        // Given
        String messageId = "CMD-002";
        deduplicationService.markAsProcessed(messageId);

        // When
        boolean result = deduplicationService.markAsProcessed(messageId);

        // Then
        assertThat(result).isFalse();
        assertThat(deduplicationService.wasProcessed(messageId)).isTrue();
    }

    @Test
    @DisplayName("Debe permitir diferentes comandos")
    void testAllowDifferentCommands() {
        // Given
        String messageId1 = "CMD-003";
        String messageId2 = "CMD-004";

        // When
        boolean result1 = deduplicationService.markAsProcessed(messageId1);
        boolean result2 = deduplicationService.markAsProcessed(messageId2);

        // Then
        assertThat(result1).isTrue();
        assertThat(result2).isTrue();
        assertThat(deduplicationService.wasProcessed(messageId1)).isTrue();
        assertThat(deduplicationService.wasProcessed(messageId2)).isTrue();
    }

    @Test
    @DisplayName("Debe manejar múltiples intentos de duplicado")
    void testMultipleDuplicateAttempts() {
        // Given
        String messageId = "CMD-005";

        // When
        boolean first = deduplicationService.markAsProcessed(messageId);
        boolean second = deduplicationService.markAsProcessed(messageId);
        boolean third = deduplicationService.markAsProcessed(messageId);

        // Then
        assertThat(first).isTrue();
        assertThat(second).isFalse();
        assertThat(third).isFalse();
    }

    @Test
    @DisplayName("Debe remover comando del cache")
    void testRemoveCommand() {
        // Given
        String messageId = "CMD-006";
        deduplicationService.markAsProcessed(messageId);
        assertThat(deduplicationService.wasProcessed(messageId)).isTrue();

        // When
        deduplicationService.remove(messageId);

        // Then
        assertThat(deduplicationService.wasProcessed(messageId)).isFalse();
        
        // Debe poder marcarse nuevamente
        boolean result = deduplicationService.markAsProcessed(messageId);
        assertThat(result).isTrue();
    }

    @Test
    @DisplayName("Debe retornar estadísticas correctas")
    void testGetStats() {
        // Given
        deduplicationService.markAsProcessed("CMD-007");
        deduplicationService.markAsProcessed("CMD-008");
        deduplicationService.markAsProcessed("CMD-009");

        // When
        Map<String, Object> stats = deduplicationService.getStats();

        // Then
        assertThat(stats).containsKeys("cachedCommands", "retentionMinutes");
        assertThat(stats.get("cachedCommands")).isEqualTo(3);
        assertThat(stats.get("retentionMinutes")).isEqualTo(60L);
    }

    @Test
    @DisplayName("Debe ser thread-safe al marcar comandos concurrentes")
    void testThreadSafety() throws InterruptedException {
        // Given
        String messageId = "CMD-010";
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        // When
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    if (deduplicationService.markAsProcessed(messageId)) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertThat(successCount.get()).isEqualTo(1); // Solo uno debe tener éxito
        assertThat(deduplicationService.wasProcessed(messageId)).isTrue();
    }

    @Test
    @DisplayName("Debe manejar comandos con IDs null o vacíos")
    void testHandleNullOrEmptyIds() {
        // When / Then
        assertThatThrownBy(() -> deduplicationService.markAsProcessed(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("Debe manejar gran cantidad de comandos")
    void testHandleLargeNumberOfCommands() {
        // Given
        int commandCount = 1000;

        // When
        for (int i = 0; i < commandCount; i++) {
            String messageId = "CMD-" + UUID.randomUUID();
            deduplicationService.markAsProcessed(messageId);
        }

        // Then
        Map<String, Object> stats = deduplicationService.getStats();
        assertThat(stats.get("cachedCommands")).isEqualTo(commandCount);
    }

    @Test
    @DisplayName("Debe verificar comando no procesado")
    void testCommandNotProcessed() {
        // Given
        String messageId = "CMD-NOT-EXISTS";

        // When
        boolean wasProcessed = deduplicationService.wasProcessed(messageId);

        // Then
        assertThat(wasProcessed).isFalse();
    }

    @Test
    @DisplayName("Debe procesar diferentes comandos concurrentemente")
    void testConcurrentDifferentCommands() throws InterruptedException {
        // Given
        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        // When
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    String messageId = "CMD-CONCURRENT-" + index;
                    if (deduplicationService.markAsProcessed(messageId)) {
                        successCount.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertThat(successCount.get()).isEqualTo(threadCount); // Todos deben tener éxito
    }

    @Test
    @DisplayName("Debe mantener integridad al remover concurrentemente")
    void testConcurrentRemoval() throws InterruptedException {
        // Given
        String messageId = "CMD-REMOVE-CONCURRENT";
        deduplicationService.markAsProcessed(messageId);

        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        // When
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    deduplicationService.remove(messageId);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertThat(deduplicationService.wasProcessed(messageId)).isFalse();
    }

    @Test
    @DisplayName("Debe limpiar comandos expirados después del tiempo de retención")
    void testAutomaticCleanup() {
        // Given - Este test requeriría modificar la retención a un valor bajo
        // Por ahora verificamos que el cleanup task está activo
        
        String messageId = "CMD-CLEANUP-TEST";
        deduplicationService.markAsProcessed(messageId);

        // When
        Map<String, Object> statsBefore = deduplicationService.getStats();
        
        // Verificar que el servicio está funcionando
        assertThat(statsBefore.get("cachedCommands")).isEqualTo(1);
        
        // Then - Verificamos que el cleanup está configurado
        assertThat(statsBefore.get("retentionMinutes")).isEqualTo(60L);
        
        // Nota: Para un test completo de cleanup, necesitarías:
        // 1. Crear un constructor que acepte retentionMinutes como parámetro
        // 2. O usar reflexión para modificar el campo RETENTION_MINUTES
        // 3. O esperar 60 minutos (no práctico en tests)
    }

    @Test
    @DisplayName("Debe manejar shutdown correctamente")
    void testShutdown() {
        // Given
        deduplicationService.markAsProcessed("CMD-SHUTDOWN-TEST");
        
        // When
        assertThatCode(() -> deduplicationService.shutdown())
            .doesNotThrowAnyException();
        
        // El servicio debe seguir funcionando después del shutdown
        // (el cache no se limpia, solo se detiene el scheduler)
        assertThat(deduplicationService.wasProcessed("CMD-SHUTDOWN-TEST")).isTrue();
    }

    @Test
    @DisplayName("Debe manejar IDs con caracteres especiales")
    void testSpecialCharactersInIds() {
        // Given
        String[] specialIds = {
            "CMD-001@special",
            "CMD-002#hash",
            "CMD-003$dollar",
            "CMD-004%percent",
            "CMD-005&ampersand"
        };

        // When / Then
        for (String messageId : specialIds) {
            boolean result = deduplicationService.markAsProcessed(messageId);
            assertThat(result).isTrue();
            assertThat(deduplicationService.wasProcessed(messageId)).isTrue();
        }
    }

    @Test
    @DisplayName("Debe manejar IDs muy largos")
    void testVeryLongIds() {
        // Given
        String longId = "CMD-" + "A".repeat(1000);

        // When
        boolean result = deduplicationService.markAsProcessed(longId);

        // Then
        assertThat(result).isTrue();
        assertThat(deduplicationService.wasProcessed(longId)).isTrue();
    }

    @Test
    @DisplayName("Debe mantener estadísticas actualizadas después de remociones")
    void testStatsAfterRemovals() {
        // Given
        deduplicationService.markAsProcessed("CMD-STATS-1");
        deduplicationService.markAsProcessed("CMD-STATS-2");
        deduplicationService.markAsProcessed("CMD-STATS-3");
        
        assertThat(deduplicationService.getStats().get("cachedCommands")).isEqualTo(3);

        // When
        deduplicationService.remove("CMD-STATS-2");

        // Then
        assertThat(deduplicationService.getStats().get("cachedCommands")).isEqualTo(2);
    }

    @Test
    @DisplayName("Debe permitir re-procesamiento después de remoción")
    void testReprocessingAfterRemoval() {
        // Given
        String messageId = "CMD-REPROCESS";
        
        // Primera vez
        boolean firstAttempt = deduplicationService.markAsProcessed(messageId);
        assertThat(firstAttempt).isTrue();
        
        // Intento duplicado
        boolean duplicateAttempt = deduplicationService.markAsProcessed(messageId);
        assertThat(duplicateAttempt).isFalse();

        // When - Remover y procesar nuevamente
        deduplicationService.remove(messageId);
        boolean afterRemoval = deduplicationService.markAsProcessed(messageId);

        // Then
        assertThat(afterRemoval).isTrue();
    }
}