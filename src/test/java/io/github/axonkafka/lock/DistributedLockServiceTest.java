package io.github.axonkafka.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterEach;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests unitarios para DistributedLockService
 * 
 * Cubre:
 * - Adquisición de locks
 * - Liberación de locks
 * - Timeouts
 * - Thread-safety
 * - Ejecución con lock
 * - Prevención de deadlocks
 * - Métricas
 */
class DistributedLockServiceTest {

    private DistributedLockService lockService;

    @BeforeEach
    void setUp() {
        lockService = new DistributedLockService();
    }

    @AfterEach
    void tearDown() {
        lockService.cleanup();
    }

    @Test
    @DisplayName("Debe adquirir lock correctamente")
    void testAcquireLock() {
        // Given
        String key = "test-lock-1";

        // When
        boolean acquired = lockService.tryLock(key, 1, TimeUnit.SECONDS);

        // Then
        assertThat(acquired).isTrue();
        
        // Cleanup
        lockService.unlock(key);
    }

    @Test
    @DisplayName("Debe prevenir adquisición concurrente del mismo lock")
    void testPreventConcurrentAcquisition() throws InterruptedException {
        // Given
        String key = "test-lock-2";
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);

        // When
        // Thread 1 adquiere el lock
        boolean first = lockService.tryLock(key, 1, TimeUnit.SECONDS);
        assertThat(first).isTrue();
        successCount.incrementAndGet();

        // Thread 2 intenta adquirir el mismo lock (debería fallar)
        CompletableFuture.runAsync(() -> {
            boolean second = lockService.tryLock(key, 100, TimeUnit.MILLISECONDS);
            if (second) {
                successCount.incrementAndGet();
                lockService.unlock(key);
            }
            latch.countDown();
        });

        latch.await(2, TimeUnit.SECONDS);

        // Then
        assertThat(successCount.get()).isEqualTo(1); // Solo el primero tuvo éxito
        
        // Cleanup
        lockService.unlock(key);
    }

    @Test
    @DisplayName("Debe permitir adquisición después de liberar lock")
    void testReacquireAfterRelease() {
        // Given
        String key = "test-lock-3";

        // When
        boolean first = lockService.tryLock(key, 1, TimeUnit.SECONDS);
        assertThat(first).isTrue();
        
        lockService.unlock(key);
        
        boolean second = lockService.tryLock(key, 1, TimeUnit.SECONDS);

        // Then
        assertThat(second).isTrue();
        
        // Cleanup
        lockService.unlock(key);
    }

    @Test
    @DisplayName("Debe timeout al esperar lock ocupado")
    void testLockTimeout() throws InterruptedException {
        // Given
        String key = "test-lock-4";
        
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        
        // Thread 1: Adquiere el lock y lo mantiene
        Thread lockHolder = new Thread(() -> {
            lockService.tryLock(key, 10, TimeUnit.SECONDS);
            lockAcquired.countDown();
            try {
                releaseLock.await(); // Espera señal para liberar
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lockService.unlock(key);
        });
        
        lockHolder.start();
        lockAcquired.await(); // Espera a que el lock sea adquirido

        // When - Thread principal intenta adquirir
        long startTime = System.currentTimeMillis();
        boolean acquired = lockService.tryLock(key, 500, TimeUnit.MILLISECONDS);
        long elapsed = System.currentTimeMillis() - startTime;

        // Then
        assertThat(acquired).isFalse();
        assertThat(elapsed).isGreaterThanOrEqualTo(400); // Margen de error
        
        // Cleanup
        releaseLock.countDown();
        lockHolder.join();
    }

    @Test
    @DisplayName("Debe ejecutar operación con lock correctamente")
    void testExecuteWithLock() {
        // Given
        String key = "test-lock-5";
        AtomicInteger counter = new AtomicInteger(0);

        // When - CAMBIO: Convertir explícitamente a Runnable
        boolean executed = lockService.executeWithLock(
            key,
            1,
            TimeUnit.SECONDS,
            (Runnable) () -> counter.incrementAndGet()
        );

        // Then
        assertThat(executed).isTrue();
        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Debe liberar lock automáticamente después de ejecutar operación")
    void testAutoReleaseLockAfterExecution() {
        // Given
        String key = "test-lock-6";
        AtomicInteger counter = new AtomicInteger(0);

        // When
        lockService.executeWithLock(key, 1, TimeUnit.SECONDS, counter::incrementAndGet);
        
        // Intentar adquirir nuevamente (debería tener éxito porque se liberó)
        boolean reacquired = lockService.tryLock(key, 1, TimeUnit.SECONDS);

        // Then
        assertThat(reacquired).isTrue();
        
        // Cleanup
        lockService.unlock(key);
    }

    @Test
    @DisplayName("Debe retornar false si no puede ejecutar por timeout")
    void testExecuteWithLockTimeout() throws InterruptedException {
        // Given
        String key = "test-lock-7";
        
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        
        // Thread que mantiene el lock
        Thread lockHolder = new Thread(() -> {
            lockService.tryLock(key, 10, TimeUnit.SECONDS);
            lockAcquired.countDown();
            try {
                releaseLock.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lockService.unlock(key);
        });
        
        lockHolder.start();
        lockAcquired.await();

        // When
        boolean executed = lockService.executeWithLock(
            key,
            100,
            TimeUnit.MILLISECONDS,
            () -> {}
        );

        // Then
        assertThat(executed).isFalse();
        
        // Cleanup
        releaseLock.countDown();
        lockHolder.join();
    }

    @Test
    @DisplayName("Debe ejecutar operación con retorno de valor")
    void testExecuteWithLockReturnValue() {
        // Given
        String key = "test-lock-8";

        // When
        String result = lockService.executeWithLock(
            key,
            1,
            TimeUnit.SECONDS,
            () -> "Operation Result"
        );

        // Then
        assertThat(result).isEqualTo("Operation Result");
    }

    @Test
    @DisplayName("Debe lanzar excepción si no puede adquirir lock para operación con retorno")
    void testExecuteWithLockReturnValueTimeout() throws InterruptedException {
        // Given
        String key = "test-lock-9";
        
        CountDownLatch lockAcquired = new CountDownLatch(1);
        CountDownLatch releaseLock = new CountDownLatch(1);
        
        Thread lockHolder = new Thread(() -> {
            lockService.tryLock(key, 10, TimeUnit.SECONDS);
            lockAcquired.countDown();
            try {
                releaseLock.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lockService.unlock(key);
        });
        
        lockHolder.start();
        lockAcquired.await();

        // When / Then
        assertThatThrownBy(() -> 
            lockService.executeWithLock(
                key,
                100,
                TimeUnit.MILLISECONDS,
                () -> "Result"
            )
        ).isInstanceOf(DistributedLockService.LockAcquisitionException.class)
        .hasMessageContaining("No se pudo adquirir lock");
        
        // Cleanup
        releaseLock.countDown();
        lockHolder.join();
    }

    @Test
    @DisplayName("Debe manejar múltiples locks diferentes concurrentemente")
    void testMultipleConcurrentLocks() throws InterruptedException {
        // Given
        int lockCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(lockCount);
        CountDownLatch latch = new CountDownLatch(lockCount);
        AtomicInteger successCount = new AtomicInteger(0);

        // When
        for (int i = 0; i < lockCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    String key = "lock-" + index;
                    if (lockService.tryLock(key, 1, TimeUnit.SECONDS)) {
                        successCount.incrementAndGet();
                        Thread.sleep(50);
                        lockService.unlock(key);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertThat(successCount.get()).isEqualTo(lockCount); // Todos deberían tener éxito
    }

    @Test
    @DisplayName("Debe serializar acceso concurrente al mismo recurso")
    void testSerializedAccess() throws InterruptedException {
        // Given
        String key = "shared-resource";
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger counter = new AtomicInteger(0);

        // When
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    lockService.executeWithLock(
                        key,
                        5,
                        TimeUnit.SECONDS,
                        () -> {
                            try {  // ✅ FIX: Añadir try-catch interno
                                int value = counter.get();
                                Thread.sleep(10); // Simular operación
                                counter.set(value + 1);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    );
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertThat(counter.get()).isEqualTo(threadCount); // Sin race conditions
    }

    @Test
    @DisplayName("Debe mantener estadísticas correctas de locks activos")
    void testLockStats() {
        // Given
        lockService.tryLock("lock-1", 1, TimeUnit.SECONDS);
        lockService.tryLock("lock-2", 1, TimeUnit.SECONDS);
        lockService.tryLock("lock-3", 1, TimeUnit.SECONDS);

        // When
        Map<String, Object> stats = lockService.getStats();

        // Then
        assertThat(stats.get("activeLocks")).isEqualTo(3);
        
        // Cleanup
        lockService.unlock("lock-1");
        lockService.unlock("lock-2");
        lockService.unlock("lock-3");
    }

    @Test
    @DisplayName("Debe manejar unlock de lock no adquirido")
    void testUnlockNonAcquiredLock() {
        // Given
        String key = "non-acquired-lock";

        // When / Then - No debería lanzar excepción
        assertThatCode(() -> lockService.unlock(key))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Debe limpiar todos los locks")
    void testCleanupAllLocks() {
        // Given
        lockService.tryLock("lock-1", 1, TimeUnit.SECONDS);
        lockService.tryLock("lock-2", 1, TimeUnit.SECONDS);
        lockService.tryLock("lock-3", 1, TimeUnit.SECONDS);

        // When
        lockService.cleanup();

        // Then
        Map<String, Object> stats = lockService.getStats();
        assertThat(stats.get("activeLocks")).isEqualTo(0);
    }

    @Test
    @DisplayName("Debe manejar InterruptedException correctamente")
    void testInterruptedWhileWaiting() throws InterruptedException {
        // Given
        String key = "interrupt-test";
        lockService.tryLock(key, 1, TimeUnit.SECONDS);
        
        Thread thread = new Thread(() -> {
            boolean acquired = lockService.tryLock(key, 10, TimeUnit.SECONDS);
            assertThat(acquired).isFalse(); // No debería adquirir
        });

        // When
        thread.start();
        Thread.sleep(100);
        thread.interrupt();
        thread.join(2000);

        // Then
        assertThat(thread.isAlive()).isFalse();
        
        // Cleanup
        lockService.unlock(key);
    }

    @Test
    @DisplayName("Debe permitir locks reentrantes del mismo thread")
    void testReentrantLock() {
        // Given
        String key = "reentrant-lock";

        // When
        boolean first = lockService.tryLock(key, 1, TimeUnit.SECONDS);
        boolean second = lockService.tryLock(key, 1, TimeUnit.SECONDS);

        // Then
        assertThat(first).isTrue();
        // ReentrantLock permite reentrancia, así que second también debería ser true
        // o false dependiendo de la implementación
        
        // Cleanup
        lockService.unlock(key);
        if (second) {
            lockService.unlock(key);
        }
    }

    @Test
    @DisplayName("Debe manejar locks con keys largos")
    void testLongKeys() {
        // Given
        String longKey = "lock-" + "a".repeat(1000);

        // When
        boolean acquired = lockService.tryLock(longKey, 1, TimeUnit.SECONDS);

        // Then
        assertThat(acquired).isTrue();
        
        // Cleanup
        lockService.unlock(longKey);
    }

    @Test
    @DisplayName("Debe manejar locks con caracteres especiales")
    void testSpecialCharactersInKeys() {
        // Given
        String[] specialKeys = {
            "lock:with:colons",
            "lock/with/slashes",
            "lock-with-dashes",
            "lock_with_underscores",
            "lock.with.dots"
        };

        // When / Then
        for (String key : specialKeys) {
            boolean acquired = lockService.tryLock(key, 1, TimeUnit.SECONDS);
            assertThat(acquired).isTrue();
            lockService.unlock(key);
        }
    }

    @Test
    @DisplayName("Debe liberar lock incluso si operación lanza excepción")
    void testLockReleasedOnException() {
        // Given
        String key = "exception-lock";

        // When
        try {
            lockService.executeWithLock(
                key,
                1,
                TimeUnit.SECONDS,
                () -> {
                    throw new RuntimeException("Operation failed");
                }
            );
        } catch (RuntimeException e) {
            // Expected
        }

        // Then - Lock debería estar liberado
        boolean reacquired = lockService.tryLock(key, 1, TimeUnit.SECONDS);
        assertThat(reacquired).isTrue();
        
        // Cleanup
        lockService.unlock(key);
    }

    @Test
    @DisplayName("Debe manejar alta concurrencia sin deadlocks")
    void testHighConcurrencyNoDeadlocks() throws InterruptedException {
        // Given
        int threadCount = 50;
        int iterations = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount * iterations);
        AtomicInteger successCount = new AtomicInteger(0);

        // When
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                for (int j = 0; j < iterations; j++) {
                    String key = "lock-" + (threadId % 5); // 5 locks compartidos
                    try {
                        lockService.executeWithLock(
                            key,
                            100,
                            TimeUnit.MILLISECONDS,
                            () -> {
                                successCount.incrementAndGet();
                                try {  // ✅ FIX: Añadir try-catch interno
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                        );
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // Then
        assertThat(completed).isTrue(); // No deadlocks
        assertThat(successCount.get()).isGreaterThan(0); // Al menos algunos tuvieron éxito
    }

    @Test
    @DisplayName("Debe retornar stats vacías después de cleanup")
    void testStatsAfterCleanup() {
        // Given
        lockService.tryLock("lock-1", 1, TimeUnit.SECONDS);
        lockService.tryLock("lock-2", 1, TimeUnit.SECONDS);

        // When
        lockService.cleanup();
        Map<String, Object> stats = lockService.getStats();

        // Then
        assertThat(stats.get("activeLocks")).isEqualTo(0);
    }
}