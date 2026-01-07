package io.github.axonkafka.saga;

import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.TargetAggregateIdentifier;
import org.axonframework.modelling.saga.EndSaga;
import org.axonframework.modelling.saga.SagaEventHandler;
import org.axonframework.modelling.saga.SagaLifecycle;
import org.axonframework.modelling.saga.StartSaga;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests unitarios para SAGA Orchestration (sin Spring Boot)
 * 
 * Escenario: Orden de compra que coordina:
 * - Validación de inventario
 * - Procesamiento de pago
 * - Envío del producto
 * 
 * Con rollback/compensación si algún paso falla.
 * 
 * NOTA: Estos son tests unitarios que validan la lógica de SAGA
 * sin requerir infraestructura completa (Kafka, PostgreSQL, Spring)
 */
class OrderSagaIntegrationTest {

    private CommandGateway commandGateway;
    private OrderSaga saga;

    private static final AtomicInteger sagaStartCount = new AtomicInteger(0);
    private static final AtomicInteger sagaEndCount = new AtomicInteger(0);
    private static final AtomicInteger compensationCount = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        commandGateway = mock(CommandGateway.class);
        saga = new OrderSaga();
        
        sagaStartCount.set(0);
        sagaEndCount.set(0);
        compensationCount.set(0);

        // Mock responses para comandos
        when(commandGateway.send(any(ReserveInventoryCommand.class)))
            .thenReturn(CompletableFuture.completedFuture("INV-001"));
        
        when(commandGateway.send(any(ProcessPaymentCommand.class)))
            .thenReturn(CompletableFuture.completedFuture("PAY-001"));
    }

    @Test
    @DisplayName("Debe iniciar SAGA cuando se crea orden")
    void testSagaStart() {
        // Given
        String orderId = UUID.randomUUID().toString();
        OrderCreatedEvent event = new OrderCreatedEvent(
            orderId, "CUST-001", "PROD-123", 2, 100.00
        );

        // When
        saga.on(event, commandGateway);

        // Then
        assertThat(sagaStartCount.get()).isEqualTo(1);
        assertThat(saga.orderId).isEqualTo(orderId);
        assertThat(saga.customerId).isEqualTo("CUST-001");
        
        // Verifica que se envió comando de reservar inventario
        verify(commandGateway).send(any(ReserveInventoryCommand.class));
    }

    @Test
    @DisplayName("Debe procesar pago después de reservar inventario")
    void testPaymentAfterInventoryReserved() {
        // Given
        String orderId = UUID.randomUUID().toString();
        
        // Iniciar SAGA
        OrderCreatedEvent orderEvent = new OrderCreatedEvent(
            orderId, "CUST-001", "PROD-123", 2, 100.00
        );
        saga.on(orderEvent, commandGateway);

        // When - Inventario reservado
        InventoryReservedEvent inventoryEvent = new InventoryReservedEvent(
            "INV-001", orderId, "PROD-123", 2
        );
        saga.on(inventoryEvent);

        // Then - Debe haber enviado comando de pago
        verify(commandGateway).send(any(ProcessPaymentCommand.class));
    }

    @Test
    @DisplayName("Debe completar SAGA exitosamente cuando pago se procesa")
    void testSagaSuccessfulCompletion() {
        // Given
        String orderId = UUID.randomUUID().toString();
        
        // Iniciar SAGA
        OrderCreatedEvent orderEvent = new OrderCreatedEvent(
            orderId, "CUST-001", "PROD-123", 2, 100.00
        );
        saga.on(orderEvent, commandGateway);
        
        // Reservar inventario
        InventoryReservedEvent inventoryEvent = new InventoryReservedEvent(
            "INV-001", orderId, "PROD-123", 2
        );
        saga.on(inventoryEvent);

        // When - Pago procesado
        PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(
            "PAY-001", orderId, 100.00
        );
        saga.on(paymentEvent);

        // Then
        assertThat(sagaEndCount.get()).isEqualTo(1);
        assertThat(compensationCount.get()).isEqualTo(0);
    }

    @Test
    @DisplayName("Debe ejecutar compensación cuando el pago falla")
    void testSagaCompensationOnPaymentFailure() {
        // Given
        String orderId = UUID.randomUUID().toString();
        
        // Iniciar SAGA
        OrderCreatedEvent orderEvent = new OrderCreatedEvent(
            orderId, "CUST-002", "PROD-456", 1, 999999.99
        );
        saga.on(orderEvent, commandGateway);
        
        // Reservar inventario
        InventoryReservedEvent inventoryEvent = new InventoryReservedEvent(
            "INV-002", orderId, "PROD-456", 1
        );
        saga.on(inventoryEvent);

        // When - Pago falla
        PaymentFailedEvent paymentFailedEvent = new PaymentFailedEvent(
            "PAY-002", orderId, "Insufficient funds"
        );
        saga.on(paymentFailedEvent);

        // Then
        assertThat(compensationCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Debe manejar múltiples eventos en secuencia")
    void testMultipleEventSequence() {
        // Given
        String orderId = UUID.randomUUID().toString();

        // When - Secuencia completa
        saga.on(new OrderCreatedEvent(orderId, "CUST-001", "PROD-123", 1, 50.00), commandGateway);
        saga.on(new InventoryReservedEvent("INV-001", orderId, "PROD-123", 1));
        saga.on(new PaymentProcessedEvent("PAY-001", orderId, 50.00));

        // Then - Verifica secuencia
        verify(commandGateway).send(any(ReserveInventoryCommand.class));
        verify(commandGateway).send(any(ProcessPaymentCommand.class));
        assertThat(sagaStartCount.get()).isEqualTo(1);
        assertThat(sagaEndCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Debe mantener estado de SAGA correctamente")
    void testSagaStatePersistence() {
        // Given
        String orderId = "ORDER-PERSIST-001";
        String customerId = "CUST-PERSIST";
        String productId = "PROD-PERSIST";
        int quantity = 5;
        double amount = 250.00;

        // When
        saga.on(new OrderCreatedEvent(orderId, customerId, productId, quantity, amount), commandGateway);

        // Then - Estado debe estar guardado
        assertThat(saga.orderId).isEqualTo(orderId);
        assertThat(saga.customerId).isEqualTo(customerId);
        assertThat(saga.productId).isEqualTo(productId);
        assertThat(saga.quantity).isEqualTo(quantity);
        assertThat(saga.amount).isEqualTo(amount);
    }

    // ========== Commands ==========

    static class CreateOrderCommand {
        @TargetAggregateIdentifier
        private String orderId;
        private String customerId;
        private String productId;
        private int quantity;
        private double amount;

        public CreateOrderCommand() {}

        public CreateOrderCommand(String orderId, String customerId, String productId, 
                                 int quantity, double amount) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.productId = productId;
            this.quantity = quantity;
            this.amount = amount;
        }

        // Getters y setters
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
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
        private String customerId;
        private double amount;

        public ProcessPaymentCommand() {}

        public ProcessPaymentCommand(String paymentId, String orderId, 
                                    String customerId, double amount) {
            this.paymentId = paymentId;
            this.orderId = orderId;
            this.customerId = customerId;
            this.amount = amount;
        }

        public String getPaymentId() { return paymentId; }
        public void setPaymentId(String paymentId) { this.paymentId = paymentId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    // ========== Events ==========

    static class OrderCreatedEvent {
        private String orderId;
        private String customerId;
        private String productId;
        private int quantity;
        private double amount;

        public OrderCreatedEvent() {}

        public OrderCreatedEvent(String orderId, String customerId, String productId,
                                int quantity, double amount) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.productId = productId;
            this.quantity = quantity;
            this.amount = amount;
        }

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public String getProductId() { return productId; }
        public void setProductId(String productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    static class InventoryReservedEvent {
        private String inventoryId;
        private String orderId;
        private String productId;
        private int quantity;

        public InventoryReservedEvent() {}

        public InventoryReservedEvent(String inventoryId, String orderId, 
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

    static class PaymentProcessedEvent {
        private String paymentId;
        private String orderId;
        private double amount;

        public PaymentProcessedEvent() {}

        public PaymentProcessedEvent(String paymentId, String orderId, double amount) {
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

    static class PaymentFailedEvent {
        private String paymentId;
        private String orderId;
        private String reason;

        public PaymentFailedEvent() {}

        public PaymentFailedEvent(String paymentId, String orderId, String reason) {
            this.paymentId = paymentId;
            this.orderId = orderId;
            this.reason = reason;
        }

        public String getPaymentId() { return paymentId; }
        public void setPaymentId(String paymentId) { this.paymentId = paymentId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
    }

    static class OrderCompletedEvent {
        private String orderId;

        public OrderCompletedEvent() {}

        public OrderCompletedEvent(String orderId) {
            this.orderId = orderId;
        }

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
    }

    static class OrderCancelledEvent {
        private String orderId;
        private String reason;

        public OrderCancelledEvent() {}

        public OrderCancelledEvent(String orderId, String reason) {
            this.orderId = orderId;
            this.reason = reason;
        }

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
    }

    // ========== SAGA (Plain Java Class for Testing) ==========

    static class OrderSaga {

        private transient CommandGateway commandGateway;

        String orderId;
        String customerId;
        String productId;
        int quantity;
        double amount;

        @StartSaga
        @SagaEventHandler(associationProperty = "orderId")
        public void on(OrderCreatedEvent event, CommandGateway commandGateway) {
            this.commandGateway = commandGateway;
            this.orderId = event.getOrderId();
            this.customerId = event.getCustomerId();
            this.productId = event.getProductId();
            this.quantity = event.getQuantity();
            this.amount = event.getAmount();

            sagaStartCount.incrementAndGet();

            // Reservar inventario
            String inventoryId = "INV-" + productId;
            ReserveInventoryCommand reserveCmd = new ReserveInventoryCommand(
                inventoryId,
                orderId,
                productId,
                quantity
            );

            commandGateway.send(reserveCmd);
        }

        @SagaEventHandler(associationProperty = "orderId")
        public void on(InventoryReservedEvent event) {
            // Procesar pago
            String paymentId = "PAY-" + orderId;
            ProcessPaymentCommand paymentCmd = new ProcessPaymentCommand(
                paymentId,
                orderId,
                customerId,
                amount
            );

            commandGateway.send(paymentCmd);
        }

        @SagaEventHandler(associationProperty = "orderId")
        @EndSaga
        public void on(PaymentProcessedEvent event) {
            // Completar orden
            sagaEndCount.incrementAndGet();
        }

        @SagaEventHandler(associationProperty = "orderId")
        @EndSaga
        public void on(PaymentFailedEvent event) {
            // Compensación: Liberar inventario
            compensationCount.incrementAndGet();
            
            // Cancelar orden (simulado)
        }
    }
}