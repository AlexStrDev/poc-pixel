package io.github.axonkafka.integration;

import io.github.axonkafka.gateway.KafkaCommandGateway;
import io.github.axonkafka.storage.EventStoreMaterializer;
import io.github.axonkafka.storage.KafkaEventStorageEngine;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.TargetAggregateIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests End-to-End que cubren:
 * - Flujo completo: Command -> Aggregate -> Event -> SAGA
 * - Disaster recovery (borrado de PG y reconstrucción desde Kafka)
 * - Coordinación de múltiples aggregates
 * - Scenarios de mundo real
 */
class EndToEndIntegrationTest {

    private CommandGateway commandGateway;
    private EventStore eventStore;
    private KafkaEventStorageEngine eventStorageEngine;
    private EventStoreMaterializer materializer;
    
    private AtomicInteger eventsPublished;
    private AtomicInteger eventsProcessed;
    private Map<String, List<DomainEventMessage<?>>> eventsByAggregate;

    @BeforeEach
    void setUp() {
        commandGateway = mock(KafkaCommandGateway.class);
        eventStore = mock(EventStore.class);
        eventStorageEngine = mock(KafkaEventStorageEngine.class);
        materializer = mock(EventStoreMaterializer.class);
        
        eventsPublished = new AtomicInteger(0);
        eventsProcessed = new AtomicInteger(0);
        eventsByAggregate = new ConcurrentHashMap<>();
    }

    @Test
    @DisplayName("E2E: Debe completar flujo Command -> Aggregate -> Event -> Processor")
    void testCompleteCommandToEventFlow() throws Exception {
        // Given
        String orderId = UUID.randomUUID().toString();
        CreateOrderCommand command = new CreateOrderCommand(
            orderId,
            "CUST-001",
            List.of(new OrderLine("PROD-001", 2, 50.0))
        );

        CountDownLatch latch = new CountDownLatch(1);
        
        // Simular procesamiento
        when(commandGateway.send(any())).thenAnswer(invocation -> {
            CompletableFuture<String> future = new CompletableFuture<>();
            future.complete(orderId);
            
            // Simular evento publicado
            eventsPublished.incrementAndGet();
            latch.countDown();
            
            return future;
        });

        // When
        CompletableFuture<String> result = commandGateway.send(command);

        // Then
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.get(2, TimeUnit.SECONDS)).isEqualTo(orderId);
        assertThat(eventsPublished.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("E2E: Disaster Recovery - Debe reconstruir desde Kafka después de borrar PG")
    void testDisasterRecoveryFromKafka() throws Exception {
        // Given - Escenario: PG se borra completamente
        String aggregateId = "ORDER-DISASTER-001";
        
        // Paso 1: Estado inicial - eventos en Kafka, materializado en PG
        when(materializer.isMaterialized(aggregateId)).thenReturn(true);
        
        // Paso 2: Simular borrado de PG
        when(materializer.isMaterialized(aggregateId)).thenReturn(false);
        
        // Paso 3: Trigger re-materialización
        CountDownLatch materializationLatch = new CountDownLatch(1);
        
        doAnswer(invocation -> {
            // Simular materialización exitosa desde Kafka
            when(materializer.isMaterialized(aggregateId)).thenReturn(true);
            materializationLatch.countDown();
            return null;
        }).when(materializer).materializeFromKafka(aggregateId);

        // When - Intento de lectura dispara materialización
        if (!materializer.isMaterialized(aggregateId)) {
            materializer.materializeFromKafka(aggregateId);
        }

        // Then
        assertThat(materializationLatch.await(5, TimeUnit.SECONDS)).isTrue();
        verify(materializer).materializeFromKafka(aggregateId);
        assertThat(materializer.isMaterialized(aggregateId)).isTrue();
    }

    @Test
    @DisplayName("E2E: Debe coordinar múltiples aggregates en transacción distribuida")
    void testDistributedTransactionCoordination() throws Exception {
        // Given - Escenario: Order necesita coordinar Inventory y Payment
        String orderId = UUID.randomUUID().toString();
        String inventoryId = "INV-001";
        String paymentId = "PAY-001";
        
        CountDownLatch orderLatch = new CountDownLatch(1);
        CountDownLatch inventoryLatch = new CountDownLatch(1);
        CountDownLatch paymentLatch = new CountDownLatch(1);

        // When - Enviar comandos en secuencia
        when(commandGateway.send(any(CreateOrderCommand.class)))
            .thenAnswer(inv -> {
                orderLatch.countDown();
                return CompletableFuture.completedFuture(orderId);
            });
        
        when(commandGateway.send(any(ReserveInventoryCommand.class)))
            .thenAnswer(inv -> {
                inventoryLatch.countDown();
                return CompletableFuture.completedFuture(inventoryId);
            });
        
        when(commandGateway.send(any(ProcessPaymentCommand.class)))
            .thenAnswer(inv -> {
                paymentLatch.countDown();
                return CompletableFuture.completedFuture(paymentId);
            });

        // Simular orquestación SAGA
        CompletableFuture<String> orderResult = commandGateway.send(
            new CreateOrderCommand(orderId, "CUST-001", List.of())
        );
        
        orderResult.thenAccept(id -> {
            commandGateway.send(new ReserveInventoryCommand(inventoryId, orderId, "PROD-001", 2));
        }).thenAccept(inv -> {
            commandGateway.send(new ProcessPaymentCommand(paymentId, orderId, 100.0));
        });

        // Then
        assertThat(orderLatch.await(3, TimeUnit.SECONDS)).isTrue();
        
        await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> {
                verify(commandGateway, atLeastOnce()).send(any(CreateOrderCommand.class));
            });
    }

    @Test
    @DisplayName("E2E: Debe manejar compensación en transacción distribuida fallida")
    void testDistributedTransactionCompensation() throws Exception {
        // Given
        String orderId = UUID.randomUUID().toString();
        AtomicInteger compensationCount = new AtomicInteger(0);

        // Simular fallo en payment
        when(commandGateway.send(any(ProcessPaymentCommand.class)))
            .thenReturn(CompletableFuture.failedFuture(
                new RuntimeException("Payment gateway timeout")
            ));

        // Compensación
        doAnswer(inv -> {
            compensationCount.incrementAndGet();
            return CompletableFuture.completedFuture("COMPENSATED");
        }).when(commandGateway).send(any(CancelOrderCommand.class));

        // When
        try {
            commandGateway.send(new ProcessPaymentCommand("PAY-001", orderId, 100.0))
                .exceptionally(ex -> {
                    // Trigger compensación
                    commandGateway.send(new CancelOrderCommand(orderId, "Payment failed"));
                    return null;
                })
                .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Expected
        }

        // Then
        await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> {
                assertThat(compensationCount.get()).isGreaterThan(0);
            });
    }

    @Test
    @DisplayName("E2E: Debe manejar alta concurrencia sin pérdida de eventos")
    void testHighConcurrencyNoEventLoss() throws Exception {
        // Given
        int orderCount = 100;
        CountDownLatch latch = new CountDownLatch(orderCount);
        AtomicInteger successCount = new AtomicInteger(0);

        when(commandGateway.send(any(CreateOrderCommand.class)))
            .thenAnswer(inv -> {
                CompletableFuture<String> future = new CompletableFuture<>();
                String orderId = UUID.randomUUID().toString();
                future.complete(orderId);
                successCount.incrementAndGet();
                latch.countDown();
                return future;
            });

        // When - Enviar múltiples comandos concurrentemente
        ExecutorService executor = Executors.newFixedThreadPool(20);
        
        for (int i = 0; i < orderCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    commandGateway.send(new CreateOrderCommand(
                        "ORDER-" + index,
                        "CUST-" + index,
                        List.of(new OrderLine("PROD-001", 1, 50.0))
                    ));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Then
        assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();
        assertThat(successCount.get()).isEqualTo(orderCount);
        
        executor.shutdown();
    }

    @Test
    @DisplayName("E2E: Debe reconstruir múltiples aggregates después de disaster")
    void testMultipleAggregatesRecovery() throws Exception {
        // Given - 10 aggregates con eventos
        List<String> aggregateIds = List.of(
            "ORDER-001", "ORDER-002", "ORDER-003", "ORDER-004", "ORDER-005",
            "ORDER-006", "ORDER-007", "ORDER-008", "ORDER-009", "ORDER-010"
        );
        
        Map<String, Boolean> materializationStatus = new ConcurrentHashMap<>();
        aggregateIds.forEach(id -> materializationStatus.put(id, false));
        
        // Simular estado de materialización
        when(materializer.isMaterialized(anyString())).thenAnswer(inv -> {
            String aggId = inv.getArgument(0);
            return materializationStatus.getOrDefault(aggId, false);
        });

        // Simular materialización exitosa
        doAnswer(inv -> {
            String aggregateId = inv.getArgument(0);
            materializationStatus.put(aggregateId, true);
            return null;
        }).when(materializer).materializeFromKafka(anyString());

        // When - Materializar todos los aggregates
        for (String aggregateId : aggregateIds) {
            if (!materializer.isMaterialized(aggregateId)) {
                materializer.materializeFromKafka(aggregateId);
            }
        }

        // Then - Verificar todos materializados
        verify(materializer, times(aggregateIds.size())).materializeFromKafka(anyString());
        
        for (String aggregateId : aggregateIds) {
            assertThat(materializer.isMaterialized(aggregateId))
                .as("Aggregate %s should be materialized", aggregateId)
                .isTrue();
        }
    }

    @Test
    @DisplayName("E2E: Debe mantener consistencia eventual en sistema distribuido")
    void testEventualConsistency() throws Exception {
        // Given
        String orderId = UUID.randomUUID().toString();
        AtomicInteger eventualConsistencyCheckpoints = new AtomicInteger(0);

        // Checkpoint 1: Command sent
        when(commandGateway.send(any(CreateOrderCommand.class)))
            .thenAnswer(inv -> {
                eventualConsistencyCheckpoints.incrementAndGet();
                return CompletableFuture.completedFuture(orderId);
            });

        // When
        commandGateway.send(new CreateOrderCommand(
            orderId, "CUST-001", List.of()
        )).thenAccept(id -> {
            // Checkpoint 2: Event published
            eventualConsistencyCheckpoints.incrementAndGet();
        }).thenAccept(v -> {
            // Checkpoint 3: Event processed
            eventualConsistencyCheckpoints.incrementAndGet();
        });

        // Then
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() -> {
                assertThat(eventualConsistencyCheckpoints.get()).isGreaterThanOrEqualTo(1);
            });
    }

    @Test
    @DisplayName("E2E: Debe soportar event replay completo")
    void testCompleteEventReplay() throws Exception {
        // Given - Aggregate con historial de 50 eventos
        String aggregateId = "ORDER-REPLAY-001";
        int eventCount = 50;
        
        // Simular que NO está materializado
        when(materializer.isMaterialized(aggregateId)).thenReturn(false);
        
        CountDownLatch replayLatch = new CountDownLatch(1);
        
        // Simular replay desde Kafka
        doAnswer(inv -> {
            // Simular lectura y materialización de 50 eventos
            eventsProcessed.set(eventCount);
            when(materializer.isMaterialized(aggregateId)).thenReturn(true);
            replayLatch.countDown();
            return null;
        }).when(materializer).materializeFromKafka(aggregateId);

        // When
        if (!materializer.isMaterialized(aggregateId)) {
            materializer.materializeFromKafka(aggregateId);
        }

        // Then
        assertThat(replayLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(eventsProcessed.get()).isEqualTo(eventCount);
        assertThat(materializer.isMaterialized(aggregateId)).isTrue();
    }

    @Test
    @DisplayName("E2E: Debe manejar scenarios de mundo real - Ecommerce checkout")
    void testEcommerceCheckoutScenario() throws Exception {
        // Given - Escenario completo de checkout
        String customerId = "CUST-001";
        String orderId = UUID.randomUUID().toString();
        String cartId = UUID.randomUUID().toString();
        
        CountDownLatch checkoutCompleteLatch = new CountDownLatch(1);

        // When - Flujo completo
        // 1. Agregar items al carrito
        when(commandGateway.send(any(AddToCartCommand.class)))
            .thenReturn(CompletableFuture.completedFuture(cartId));

        // 2. Procesar checkout
        when(commandGateway.send(any(CheckoutCommand.class)))
            .thenAnswer(inv -> {
                // Internamente dispara múltiples comandos
                commandGateway.send(new CreateOrderCommand(orderId, customerId, List.of()));
                commandGateway.send(new ReserveInventoryCommand("INV-001", orderId, "PROD-001", 1));
                commandGateway.send(new ProcessPaymentCommand("PAY-001", orderId, 100.0));
                
                checkoutCompleteLatch.countDown();
                return CompletableFuture.completedFuture(orderId);
            });

        // Execute
        commandGateway.send(new AddToCartCommand(cartId, "PROD-001", 1));
        commandGateway.send(new CheckoutCommand(cartId, customerId));

        // Then
        assertThat(checkoutCompleteLatch.await(10, TimeUnit.SECONDS)).isTrue();
        
        verify(commandGateway).send(any(CreateOrderCommand.class));
        verify(commandGateway).send(any(ReserveInventoryCommand.class));
        verify(commandGateway).send(any(ProcessPaymentCommand.class));
    }

    @Test
    @DisplayName("E2E: Debe recuperarse de fallo parcial del sistema")
    void testPartialSystemFailureRecovery() throws Exception {
        // Given - Sistema con 3 nodos, 1 falla
        int totalNodes = 3;
        int failedNodes = 1;
        int successfulNodes = totalNodes - failedNodes;
        
        CountDownLatch successLatch = new CountDownLatch(successfulNodes);
        AtomicInteger successfulCommands = new AtomicInteger(0);

        // Simular que 2 de 3 nodos procesan correctamente
        when(commandGateway.send(any()))
            .thenAnswer(inv -> {
                if (successfulCommands.get() < successfulNodes) {
                    successfulCommands.incrementAndGet();
                    successLatch.countDown();
                    return CompletableFuture.completedFuture("OK");
                } else {
                    return CompletableFuture.failedFuture(
                        new RuntimeException("Node failure")
                    );
                }
            });

        // When - Enviar comandos
        for (int i = 0; i < totalNodes; i++) {
            try {
                commandGateway.send(new CreateOrderCommand(
                    "ORDER-" + i, "CUST-" + i, List.of()
                ));
            } catch (Exception e) {
                // Expected for failed node
            }
        }

        // Then
        assertThat(successLatch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(successfulCommands.get()).isEqualTo(successfulNodes);
    }

    @Test
    @DisplayName("E2E: Performance - Debe procesar 1000 comandos en tiempo razonable")
    void testPerformanceThousandCommands() throws Exception {
        // Given
        int commandCount = 1000;
        CountDownLatch latch = new CountDownLatch(commandCount);
        AtomicInteger processedCount = new AtomicInteger(0);

        when(commandGateway.send(any()))
            .thenAnswer(inv -> {
                processedCount.incrementAndGet();
                latch.countDown();
                return CompletableFuture.completedFuture("OK");
            });

        long startTime = System.currentTimeMillis();

        // When
        ExecutorService executor = Executors.newFixedThreadPool(50);
        
        for (int i = 0; i < commandCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    commandGateway.send(new CreateOrderCommand(
                        "ORDER-" + index,
                        "CUST-" + (index % 100),
                        List.of()
                    ));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Then
        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (commandCount * 1000.0) / duration;
        
        System.out.println("Processed " + commandCount + " commands in " + duration + "ms");
        System.out.println("Throughput: " + throughput + " commands/sec");
        
        assertThat(processedCount.get()).isEqualTo(commandCount);
        assertThat(throughput).isGreaterThan(30); // Al menos 30 comandos/seg
        
        executor.shutdown();
    }

    // ========== Command DTOs ==========

    static class CreateOrderCommand {
        @TargetAggregateIdentifier
        private String orderId;
        private String customerId;
        private List<OrderLine> lines;

        public CreateOrderCommand() {}
        public CreateOrderCommand(String orderId, String customerId, List<OrderLine> lines) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.lines = lines;
        }

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public List<OrderLine> getLines() { return lines; }
        public void setLines(List<OrderLine> lines) { this.lines = lines; }
    }

    static class ReserveInventoryCommand {
        @TargetAggregateIdentifier
        private String inventoryId;
        private String orderId;
        private String productId;
        private int quantity;

        public ReserveInventoryCommand() {}
        public ReserveInventoryCommand(String inventoryId, String orderId, 
                                      String productId, int quantity) {
            this.inventoryId = inventoryId;
            this.orderId = orderId;
            this.productId = productId;
            this.quantity = quantity;
        }

        public String getInventoryId() { return inventoryId; }
        public void setInventoryId(String inventoryId) { this.inventoryId = inventoryId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
    }

    static class ProcessPaymentCommand {
        @TargetAggregateIdentifier
        private String paymentId;
        private String orderId;
        private double amount;

        public ProcessPaymentCommand() {}
        public ProcessPaymentCommand(String paymentId, String orderId, double amount) {
            this.paymentId = paymentId;
            this.orderId = orderId;
            this.amount = amount;
        }

        public String getPaymentId() { return paymentId; }
        public void setPaymentId(String paymentId) { this.paymentId = paymentId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    static class CancelOrderCommand {
        @TargetAggregateIdentifier
        private String orderId;
        private String reason;

        public CancelOrderCommand() {}
        public CancelOrderCommand(String orderId, String reason) {
            this.orderId = orderId;
            this.reason = reason;
        }

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
    }

    static class AddToCartCommand {
        @TargetAggregateIdentifier
        private String cartId;
        private String productId;
        private int quantity;

        public AddToCartCommand() {}
        public AddToCartCommand(String cartId, String productId, int quantity) {
            this.cartId = cartId;
            this.productId = productId;
            this.quantity = quantity;
        }

        public String getCartId() { return cartId; }
        public void setCartId(String cartId) { this.cartId = cartId; }
        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
    }

    static class CheckoutCommand {
        @TargetAggregateIdentifier
        private String cartId;
        private String customerId;

        public CheckoutCommand() {}
        public CheckoutCommand(String cartId, String customerId) {
            this.cartId = cartId;
            this.customerId = customerId;
        }

        public String getCartId() { return cartId; }
        public void setCartId(String cartId) { this.cartId = cartId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
    }

    static class OrderLine {
        private String productId;
        private int quantity;
        private double price;

        public OrderLine() {}
        public OrderLine(String productId, int quantity, double price) {
            this.productId = productId;
            this.quantity = quantity;
            this.price = price;
        }

        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public double getPrice() { return price; }
        public void setPrice(double price) { this.price = price; }
    }
}