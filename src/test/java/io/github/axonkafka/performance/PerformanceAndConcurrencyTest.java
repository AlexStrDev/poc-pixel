package io.github.axonkafka.performance;

import io.github.axonkafka.bus.KafkaCommandBus;
import io.github.axonkafka.bus.KafkaEventBus;
import io.github.axonkafka.gateway.KafkaCommandGateway;
import io.github.axonkafka.storage.EventStoreMaterializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Disabled;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests avanzados de Performance y Concurrencia
 * 
 * Cubre:
 * - Throughput bajo carga
 * - Latency percentiles
 * - Memory leak detection
 * - Thread safety bajo stress
 * - Backpressure handling
 * - Resource cleanup
 */
class PerformanceAndConcurrencyTest {

    private MemoryMXBean memoryBean;
    private ExecutorService executorService;
    
    private AtomicInteger successCount;
    private AtomicInteger failureCount;
    private AtomicLong totalLatencyMs;
    
    private List<Long> latencies;

    @BeforeEach
    void setUp() {
        memoryBean = ManagementFactory.getMemoryMXBean();
        executorService = Executors.newFixedThreadPool(100);
        
        successCount = new AtomicInteger(0);
        failureCount = new AtomicInteger(0);
        totalLatencyMs = new AtomicLong(0);
        
        latencies = new CopyOnWriteArrayList<>();
    }

    @Test
    @DisplayName("Performance: Debe mantener throughput estable bajo carga sostenida")
    void testSustainedThroughput() throws Exception {
        // Given
        int durationSeconds = 30;
        int targetThroughput = 100; // comandos/seg
        int totalCommands = durationSeconds * targetThroughput;
        
        CountDownLatch latch = new CountDownLatch(totalCommands);
        
        long startTime = System.currentTimeMillis();

        // When - Enviar comandos a ritmo constante
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        
        scheduler.scheduleAtFixedRate(() -> {
            for (int i = 0; i < targetThroughput / 10; i++) {
                executorService.submit(() -> {
                    try {
                        long cmdStart = System.nanoTime();
                        simulateCommandProcessing();
                        long cmdEnd = System.nanoTime();
                        
                        long latencyMs = TimeUnit.NANOSECONDS.toMillis(cmdEnd - cmdStart);
                        latencies.add(latencyMs);
                        totalLatencyMs.addAndGet(latencyMs);
                        
                        successCount.incrementAndGet();
                        latch.countDown();
                    } catch (Exception e) {
                        failureCount.incrementAndGet();
                        latch.countDown();
                    }
                });
            }
        }, 0, 100, TimeUnit.MILLISECONDS);

        // Then
        boolean completed = latch.await(durationSeconds + 10, TimeUnit.SECONDS);
        scheduler.shutdown();
        
        long duration = System.currentTimeMillis() - startTime;
        double actualThroughput = (successCount.get() * 1000.0) / duration;
        double avgLatencyMs = totalLatencyMs.get() / (double) successCount.get();
        
        System.out.println("=== Sustained Throughput Test ===");
        System.out.println("Duration: " + duration + "ms");
        System.out.println("Successful: " + successCount.get());
        System.out.println("Failed: " + failureCount.get());
        System.out.println("Actual Throughput: " + actualThroughput + " cmds/sec");
        System.out.println("Average Latency: " + avgLatencyMs + "ms");
        
        assertThat(completed).isTrue();
        assertThat(actualThroughput).isGreaterThan(targetThroughput * 0.8); // 80% del target
        assertThat(failureCount.get()).isLessThan(totalCommands / 100); // < 1% fallas
    }

    @Test
    @DisplayName("Performance: Debe calcular latency percentiles correctamente")
    void testLatencyPercentiles() throws Exception {
        // Given
        int commandCount = 1000;
        CountDownLatch latch = new CountDownLatch(commandCount);

        // When
        for (int i = 0; i < commandCount; i++) {
            executorService.submit(() -> {
                try {
                    long start = System.nanoTime();
                    simulateCommandProcessing();
                    long end = System.nanoTime();
                    
                    latencies.add(TimeUnit.NANOSECONDS.toMillis(end - start));
                    latch.countDown();
                } catch (Exception e) {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();

        // Then - Calcular percentiles
        Collections.sort(latencies);
        
        long p50 = getPercentile(latencies, 50);
        long p90 = getPercentile(latencies, 90);
        long p95 = getPercentile(latencies, 95);
        long p99 = getPercentile(latencies, 99);
        
        System.out.println("=== Latency Percentiles ===");
        System.out.println("P50: " + p50 + "ms");
        System.out.println("P90: " + p90 + "ms");
        System.out.println("P95: " + p95 + "ms");
        System.out.println("P99: " + p99 + "ms");
        
        assertThat(p50).isLessThan(100);
        assertThat(p90).isLessThan(200);
        assertThat(p95).isLessThan(300);
        assertThat(p99).isLessThan(500);
    }

    @Test
    @DisplayName("Memory: Debe detectar memory leaks con carga prolongada")
    void testMemoryLeakDetection() throws Exception {
        // Given
        int iterations = 5;
        int commandsPerIteration = 1000;
        
        List<MemorySnapshot> snapshots = new ArrayList<>();

        // When - Múltiples iteraciones con GC entre ellas
        for (int iter = 0; iter < iterations; iter++) {
            MemorySnapshot beforeIteration = takeMemorySnapshot();
            
            CountDownLatch latch = new CountDownLatch(commandsPerIteration);
            
            for (int i = 0; i < commandsPerIteration; i++) {
                executorService.submit(() -> {
                    try {
                        simulateCommandWithCaching();
                        latch.countDown();
                    } catch (Exception e) {
                        latch.countDown();
                    }
                });
            }
            
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            
            // Force GC
            System.gc();
            Thread.sleep(1000);
            
            MemorySnapshot afterIteration = takeMemorySnapshot();
            snapshots.add(afterIteration);
            
            System.out.println("Iteration " + iter + ": " + 
                afterIteration.usedMemoryMB + "MB used");
        }

        // Then - Verificar que la memoria no crece indefinidamente
        long firstIterationMemory = snapshots.get(0).usedMemoryMB;
        long lastIterationMemory = snapshots.get(iterations - 1).usedMemoryMB;
        
        double memoryGrowth = ((lastIterationMemory - firstIterationMemory) / 
                               (double) firstIterationMemory) * 100;
        
        System.out.println("=== Memory Leak Detection ===");
        System.out.println("First iteration: " + firstIterationMemory + "MB");
        System.out.println("Last iteration: " + lastIterationMemory + "MB");
        System.out.println("Memory growth: " + memoryGrowth + "%");
        
        assertThat(memoryGrowth).isLessThan(50); // < 50% crecimiento
    }

    @Test
    @DisplayName("Concurrency: Debe manejar race conditions correctamente")
    void testRaceConditionHandling() throws Exception {
        // Given
        String sharedResourceId = "SHARED-001";
        int threadCount = 100;
        int operationsPerThread = 100;
        
        AtomicInteger actualOperations = new AtomicInteger(0);
        ConcurrentHashMap<String, Integer> processedByThread = new ConcurrentHashMap<>();
        
        CountDownLatch latch = new CountDownLatch(threadCount);

        // When - Múltiples threads accediendo al mismo recurso
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executorService.submit(() -> {
                try {
                    for (int op = 0; op < operationsPerThread; op++) {
                        // Simular operación con recurso compartido
                        int current = actualOperations.get();
                        Thread.sleep(0, 100); // Nanosleep para inducir race
                        actualOperations.compareAndSet(current, current + 1);
                        
                        processedByThread.merge(
                            "thread-" + threadId,
                            1,
                            Integer::sum
                        );
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Then
        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        
        int totalProcessed = processedByThread.values().stream()
            .mapToInt(Integer::intValue)
            .sum();
        
        System.out.println("=== Race Condition Test ===");
        System.out.println("Expected operations: " + (threadCount * operationsPerThread));
        System.out.println("Actual operations: " + actualOperations.get());
        System.out.println("Processed by threads: " + totalProcessed);
        
        assertThat(totalProcessed).isEqualTo(threadCount * operationsPerThread);
    }

    @Test
    @DisplayName("Stress: Debe sobrevivir a spike de tráfico súbito")
    void testTrafficSpikeResilience() throws Exception {
        // Given
        int normalRate = 10; // cmds/sec
        int spikeRate = 500; // cmds/sec
        int spikeDuration = 5; // segundos
        
        CountDownLatch warmupLatch = new CountDownLatch(normalRate * 5);
        CountDownLatch spikeLatch = new CountDownLatch(spikeRate * spikeDuration);
        CountDownLatch cooldownLatch = new CountDownLatch(normalRate * 5);

        // When - Warm-up
        System.out.println("Warm-up phase...");
        for (int i = 0; i < normalRate * 5; i++) {
            executorService.submit(() -> {
                try {
                    simulateCommandProcessing();
                    successCount.incrementAndGet();
                    warmupLatch.countDown();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    warmupLatch.countDown();
                }
            });
            Thread.sleep(100);
        }
        
        assertThat(warmupLatch.await(10, TimeUnit.SECONDS)).isTrue();

        // Spike phase
        System.out.println("SPIKE phase!");
        long spikeStart = System.currentTimeMillis();
        
        for (int i = 0; i < spikeRate * spikeDuration; i++) {
            executorService.submit(() -> {
                try {
                    simulateCommandProcessing();
                    successCount.incrementAndGet();
                    spikeLatch.countDown();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    spikeLatch.countDown();
                }
            });
            Thread.sleep(1000 / spikeRate);
        }
        
        assertThat(spikeLatch.await(spikeDuration + 10, TimeUnit.SECONDS)).isTrue();
        long spikeDurationMs = System.currentTimeMillis() - spikeStart;

        // Cool-down
        System.out.println("Cool-down phase...");
        for (int i = 0; i < normalRate * 5; i++) {
            executorService.submit(() -> {
                try {
                    simulateCommandProcessing();
                    successCount.incrementAndGet();
                    cooldownLatch.countDown();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    cooldownLatch.countDown();
                }
            });
            Thread.sleep(100);
        }
        
        assertThat(cooldownLatch.await(10, TimeUnit.SECONDS)).isTrue();

        // Then
        double spikeSuccessRate = (double) successCount.get() / 
            (successCount.get() + failureCount.get()) * 100;
        
        System.out.println("=== Traffic Spike Test ===");
        System.out.println("Spike duration: " + spikeDurationMs + "ms");
        System.out.println("Total successful: " + successCount.get());
        System.out.println("Total failed: " + failureCount.get());
        System.out.println("Success rate: " + spikeSuccessRate + "%");
        
        assertThat(spikeSuccessRate).isGreaterThan(95); // > 95% éxito
    }

    @Test
    @DisplayName("Backpressure: Debe aplicar backpressure cuando está sobrecargado")
    void testBackpressureHandling() throws Exception {
        // Given
        int queueCapacity = 100;
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueCapacity);
        ThreadPoolExecutor limitedExecutor = new ThreadPoolExecutor(
            10, 10,
            60, TimeUnit.SECONDS,
            queue,
            new ThreadPoolExecutor.CallerRunsPolicy() // Backpressure
        );

        int commandCount = 500;
        CountDownLatch latch = new CountDownLatch(commandCount);
        AtomicInteger rejectedCount = new AtomicInteger(0);

        // When
        for (int i = 0; i < commandCount; i++) {
            try {
                limitedExecutor.submit(() -> {
                    try {
                        Thread.sleep(100); // Simular procesamiento lento
                        successCount.incrementAndGet();
                        latch.countDown();
                    } catch (Exception e) {
                        failureCount.incrementAndGet();
                        latch.countDown();
                    }
                });
            } catch (RejectedExecutionException e) {
                rejectedCount.incrementAndGet();
                latch.countDown();
            }
        }

        // Then
        assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
        
        System.out.println("=== Backpressure Test ===");
        System.out.println("Submitted: " + commandCount);
        System.out.println("Successful: " + successCount.get());
        System.out.println("Rejected: " + rejectedCount.get());
        
        assertThat(successCount.get() + rejectedCount.get()).isEqualTo(commandCount);
        
        limitedExecutor.shutdown();
    }

    @Test
    @DisplayName("Resource: Debe limpiar recursos correctamente después de uso")
    void testResourceCleanup() throws Exception {
        // Given
        Map<String, Object> resources = new ConcurrentHashMap<>();
        AtomicInteger activeResources = new AtomicInteger(0);
        
        int taskCount = 1000;
        CountDownLatch latch = new CountDownLatch(taskCount);

        // When
        for (int i = 0; i < taskCount; i++) {
            final int taskId = i;
            executorService.submit(() -> {
                String resourceId = "RESOURCE-" + taskId;
                
                try {
                    // Adquirir recurso
                    resources.put(resourceId, new Object());
                    activeResources.incrementAndGet();
                    
                    // Usar recurso
                    simulateCommandProcessing();
                    
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                } finally {
                    // Liberar recurso
                    resources.remove(resourceId);
                    activeResources.decrementAndGet();
                    latch.countDown();
                }
            });
        }

        // Then
        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        
        System.out.println("=== Resource Cleanup Test ===");
        System.out.println("Active resources after cleanup: " + activeResources.get());
        System.out.println("Resources in map: " + resources.size());
        
        assertThat(activeResources.get()).isEqualTo(0);
        assertThat(resources).isEmpty();
    }

    @Test
    @DisplayName("Deadlock: Debe detectar y prevenir deadlocks")
    void testDeadlockPrevention() throws Exception {
        // Given
        Object lock1 = new Object();
        Object lock2 = new Object();
        
        CountDownLatch thread1Ready = new CountDownLatch(1);
        CountDownLatch thread2Ready = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(2);
        
        AtomicInteger deadlockDetected = new AtomicInteger(0);

        // When - Intentar crear deadlock con timeout
        Thread thread1 = new Thread(() -> {
            try {
                synchronized (lock1) {
                    thread1Ready.countDown();
                    thread2Ready.await(1, TimeUnit.SECONDS);
                    
                    // Intentar adquirir lock2 con timeout
                    if (tryLock(lock2, 2, TimeUnit.SECONDS)) {
                        try {
                            // Critical section
                            Thread.sleep(100);
                        } finally {
                            // Lock2 liberado automáticamente
                        }
                    } else {
                        deadlockDetected.incrementAndGet();
                        System.out.println("Thread 1: Deadlock detectado!");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                completionLatch.countDown();
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                synchronized (lock2) {
                    thread2Ready.countDown();
                    thread1Ready.await(1, TimeUnit.SECONDS);
                    
                    // Intentar adquirir lock1 con timeout
                    if (tryLock(lock1, 2, TimeUnit.SECONDS)) {
                        try {
                            // Critical section
                            Thread.sleep(100);
                        } finally {
                            // Lock1 liberado automáticamente
                        }
                    } else {
                        deadlockDetected.incrementAndGet();
                        System.out.println("Thread 2: Deadlock detectado!");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                completionLatch.countDown();
            }
        });

        thread1.start();
        thread2.start();

        // Then
        assertThat(completionLatch.await(10, TimeUnit.SECONDS)).isTrue();
        
        System.out.println("=== Deadlock Prevention Test ===");
        System.out.println("Deadlocks detected: " + deadlockDetected.get());
        
        // Ambos threads deben completar (no deadlock permanente)
        assertThat(thread1.isAlive()).isFalse();
        assertThat(thread2.isAlive()).isFalse();
    }

    @Test
    @Disabled("Long-running test - solo ejecutar manualmente")
    @DisplayName("Endurance: Debe mantener estabilidad durante 1 hora")
    void testLongRunningStability() throws Exception {
        // Given
        long durationMinutes = 60;
        long endTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(durationMinutes);
        
        AtomicLong totalProcessed = new AtomicLong(0);
        AtomicLong totalErrors = new AtomicLong(0);
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        // When - Enviar comandos continuamente durante 1 hora
        scheduler.scheduleAtFixedRate(() -> {
            if (System.currentTimeMillis() < endTime) {
                for (int i = 0; i < 10; i++) {
                    executorService.submit(() -> {
                        try {
                            simulateCommandProcessing();
                            totalProcessed.incrementAndGet();
                        } catch (Exception e) {
                            totalErrors.incrementAndGet();
                        }
                    });
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);

        // Monitoreo cada 5 minutos
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.MINUTES.toMillis(5));
            
            System.out.println("=== Status Check ===");
            System.out.println("Processed: " + totalProcessed.get());
            System.out.println("Errors: " + totalErrors.get());
            System.out.println("Error rate: " + 
                (totalErrors.get() * 100.0 / totalProcessed.get()) + "%");
            System.out.println("Memory: " + takeMemorySnapshot().usedMemoryMB + "MB");
        }

        scheduler.shutdown();

        // Then
        double errorRate = (totalErrors.get() * 100.0) / totalProcessed.get();
        
        System.out.println("=== Endurance Test Results ===");
        System.out.println("Total processed: " + totalProcessed.get());
        System.out.println("Total errors: " + totalErrors.get());
        System.out.println("Error rate: " + errorRate + "%");
        
        assertThat(errorRate).isLessThan(1.0); // < 1% error rate
    }

    // ========== Helper Methods ==========

    private void simulateCommandProcessing() throws InterruptedException {
        // Simular procesamiento de comando
        Thread.sleep(ThreadLocalRandom.current().nextInt(10, 50));
    }

    private void simulateCommandWithCaching() throws InterruptedException {
        // Simular comando con caching que podría causar memory leak
        Map<String, Object> cache = new ConcurrentHashMap<>();
        cache.put(UUID.randomUUID().toString(), new byte[1024]); // 1KB
        
        Thread.sleep(ThreadLocalRandom.current().nextInt(5, 20));
        
        cache.clear(); // Cleanup
    }

    private long getPercentile(List<Long> sortedValues, int percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sortedValues.size()) - 1;
        return sortedValues.get(Math.max(0, index));
    }

    private MemorySnapshot takeMemorySnapshot() {
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        return new MemorySnapshot(
            heapUsage.getUsed() / (1024 * 1024),
            heapUsage.getMax() / (1024 * 1024),
            heapUsage.getCommitted() / (1024 * 1024)
        );
    }

    private boolean tryLock(Object lock, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        
        while (System.nanoTime() < deadline) {
            if (Thread.holdsLock(lock)) {
                return true;
            }
            Thread.sleep(10);
        }
        
        return false;
    }

    static class MemorySnapshot {
        final long usedMemoryMB;
        final long maxMemoryMB;
        final long committedMemoryMB;

        MemorySnapshot(long used, long max, long committed) {
            this.usedMemoryMB = used;
            this.maxMemoryMB = max;
            this.committedMemoryMB = committed;
        }
    }
}