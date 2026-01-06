package io.github.axonkafka.bus;

import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.KafkaEventSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests de integración para KafkaEventBus
 * 
 * Cubre:
 * - Publicación de eventos a Kafka
 * - Consumo de eventos desde Kafka
 * - Registro de handlers
 * - Manejo de errores
 * - Thread-safety
 */
class KafkaEventBusIntegrationTest {

    private KafkaEventBus eventBus;
    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaEventSerializer eventSerializer;
    private AxonKafkaProperties properties;

    static class OrderCreatedEvent {
        private String orderId;
        private String customerId;
        private double amount;

        public OrderCreatedEvent() {}

        public OrderCreatedEvent(String orderId, String customerId, double amount) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.amount = amount;
        }

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    static class OrderConfirmedEvent {
        private String orderId;
        public OrderConfirmedEvent() {}
        public OrderConfirmedEvent(String orderId) { this.orderId = orderId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
    }

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        eventSerializer = new KafkaEventSerializer();
        
        properties = new AxonKafkaProperties();
        properties.getEvent().setTopic("test-events");
        properties.getEvent().setGroupId("test-event-processors");

        eventBus = new KafkaEventBus(kafkaTemplate, eventSerializer, properties);
    }

    @Test
    @DisplayName("Debe publicar evento a Kafka correctamente")
    void testPublishEvent() {
        // Given
        String aggregateId = "ORDER-001";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-123", 150.50);
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(null));

        // When
        eventBus.publish(eventMessage);

        // Then
        verify(kafkaTemplate).send(
            eq("test-events"),
            eq(aggregateId),
            anyString()
        );
    }

    @Test
    @DisplayName("Debe lanzar excepción si falla publicación a Kafka")
    void testPublishEventFailure() {
        // Given
        String aggregateId = "ORDER-002";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-456", 200.75);
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenThrow(new RuntimeException("Kafka connection error"));

        // When / Then
        assertThatThrownBy(() -> eventBus.publish(eventMessage))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Error publicando evento");
    }

    @Test
    @DisplayName("Debe consumir y procesar evento desde Kafka")
    void testConsumeEvent() throws Exception {
        // Given
        String aggregateId = "ORDER-003";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-789", 99.99);
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );
        
        String serialized = eventSerializer.serialize(eventMessage);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-events", 0, 0, aggregateId, serialized
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DomainEventMessage<?>> receivedEvent = new AtomicReference<>();

        eventBus.registerEventHandler(evt -> {
            receivedEvent.set(evt);
            latch.countDown();
        });

        // When
        eventBus.consumeEvent(record, ack);

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(receivedEvent.get()).isNotNull();
        assertThat(receivedEvent.get().getAggregateIdentifier()).isEqualTo(aggregateId);
        
        OrderCreatedEvent receivedPayload = (OrderCreatedEvent) receivedEvent.get().getPayload();
        assertThat(receivedPayload.getOrderId()).isEqualTo(aggregateId);
        assertThat(receivedPayload.getAmount()).isEqualTo(99.99);
        
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Debe registrar múltiples handlers")
    void testRegisterMultipleHandlers() throws Exception {
        // Given
        String aggregateId = "ORDER-004";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-111", 123.45);
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );
        
        String serialized = eventSerializer.serialize(eventMessage);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-events", 0, 0, aggregateId, serialized
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger handler1Count = new AtomicInteger(0);
        AtomicInteger handler2Count = new AtomicInteger(0);
        AtomicInteger handler3Count = new AtomicInteger(0);

        eventBus.registerEventHandler(evt -> {
            handler1Count.incrementAndGet();
            latch.countDown();
        });
        
        eventBus.registerEventHandler(evt -> {
            handler2Count.incrementAndGet();
            latch.countDown();
        });
        
        eventBus.registerEventHandler(evt -> {
            handler3Count.incrementAndGet();
            latch.countDown();
        });

        // When
        eventBus.consumeEvent(record, ack);

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(handler1Count.get()).isEqualTo(1);
        assertThat(handler2Count.get()).isEqualTo(1);
        assertThat(handler3Count.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Debe manejar errores en handlers sin detener otros handlers")
    void testHandlerErrorDoesNotStopOtherHandlers() throws Exception {
        // Given
        String aggregateId = "ORDER-005";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-222", 250.00);
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );
        
        String serialized = eventSerializer.serialize(eventMessage);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-events", 0, 0, aggregateId, serialized
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        CountDownLatch latch = new CountDownLatch(2); // Solo 2, porque 1 falla
        AtomicInteger successCount = new AtomicInteger(0);

        // Handler 1: Falla
        eventBus.registerEventHandler(evt -> {
            throw new RuntimeException("Handler 1 failed");
        });
        
        // Handler 2: Exitoso
        eventBus.registerEventHandler(evt -> {
            successCount.incrementAndGet();
            latch.countDown();
        });
        
        // Handler 3: Exitoso
        eventBus.registerEventHandler(evt -> {
            successCount.incrementAndGet();
            latch.countDown();
        });

        // When
        eventBus.consumeEvent(record, ack);

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(successCount.get()).isEqualTo(2); // Otros 2 handlers tuvieron éxito
        verify(ack).acknowledge(); // Aún hace acknowledge
    }

    @Test
    @DisplayName("Debe hacer acknowledge incluso si hay errores en handlers")
    void testAcknowledgeEvenOnHandlerError() {
        // Given
        String aggregateId = "ORDER-006";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-333", 175.00);
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );
        
        String serialized = eventSerializer.serialize(eventMessage);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-events", 0, 0, aggregateId, serialized
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        eventBus.registerEventHandler(evt -> {
            throw new RuntimeException("Handler error");
        });

        // When
        eventBus.consumeEvent(record, ack);

        // Then
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Debe manejar evento sin acknowledgment")
    void testConsumeEventWithoutAcknowledgment() throws Exception {
        // Given
        String aggregateId = "ORDER-007";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-444", 300.00);
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );
        
        String serialized = eventSerializer.serialize(eventMessage);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-events", 0, 0, aggregateId, serialized
        );

        CountDownLatch latch = new CountDownLatch(1);
        eventBus.registerEventHandler(evt -> latch.countDown());

        // When
        eventBus.consumeEvent(record, null);

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("Debe publicar eventos con metadata correctamente")
    void testPublishEventWithMetadata() {
        // Given
        String aggregateId = "ORDER-008";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-555", 400.00);
        
        MetaData metadata = MetaData.with("userId", "USER-123")
            .and("correlationId", UUID.randomUUID().toString());
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            metadata
        );

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(null));

        // When
        eventBus.publish(eventMessage);

        // Then
        verify(kafkaTemplate).send(
            eq("test-events"),
            eq(aggregateId),
            contains("userId")
        );
    }

    @Test
    @DisplayName("Debe procesar eventos concurrentemente sin pérdida")
    void testConcurrentEventProcessing() throws Exception {
        // Given
        int eventCount = 50;
        CountDownLatch latch = new CountDownLatch(eventCount);
        AtomicInteger processedCount = new AtomicInteger(0);

        eventBus.registerEventHandler(evt -> {
            processedCount.incrementAndGet();
            latch.countDown();
        });

        // When
        for (int i = 0; i < eventCount; i++) {
            String aggregateId = "ORDER-" + i;
            OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-" + i, 100.0 + i);
            
            DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
                "Order",
                aggregateId,
                (long) i,
                event,
                MetaData.emptyInstance()
            );
            
            String serialized = eventSerializer.serialize(eventMessage);
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "test-events", 0, i, aggregateId, serialized
            );
            Acknowledgment ack = mock(Acknowledgment.class);

            // Procesar en threads separados para simular concurrencia
            new Thread(() -> eventBus.consumeEvent(record, ack)).start();
        }

        // Then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(processedCount.get()).isEqualTo(eventCount);
    }

    @Test
    @DisplayName("Debe manejar diferentes tipos de eventos")
    void testHandleDifferentEventTypes() throws Exception {
        // Given
        String aggregateId = "ORDER-009";
        
        OrderCreatedEvent createdEvent = new OrderCreatedEvent(aggregateId, "CUST-666", 500.00);
        DomainEventMessage<OrderCreatedEvent> createdMessage = new GenericDomainEventMessage<>(
            "Order", aggregateId, 0L, createdEvent, MetaData.emptyInstance()
        );
        
        OrderConfirmedEvent confirmedEvent = new OrderConfirmedEvent(aggregateId);
        DomainEventMessage<OrderConfirmedEvent> confirmedMessage = new GenericDomainEventMessage<>(
            "Order", aggregateId, 1L, confirmedEvent, MetaData.emptyInstance()
        );

        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger createdCount = new AtomicInteger(0);
        AtomicInteger confirmedCount = new AtomicInteger(0);

        eventBus.registerEventHandler(evt -> {
            if (evt.getPayload() instanceof OrderCreatedEvent) {
                createdCount.incrementAndGet();
            } else if (evt.getPayload() instanceof OrderConfirmedEvent) {
                confirmedCount.incrementAndGet();
            }
            latch.countDown();
        });

        // When
        String serialized1 = eventSerializer.serialize(createdMessage);
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(
            "test-events", 0, 0, aggregateId, serialized1
        );
        eventBus.consumeEvent(record1, mock(Acknowledgment.class));

        String serialized2 = eventSerializer.serialize(confirmedMessage);
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(
            "test-events", 0, 1, aggregateId, serialized2
        );
        eventBus.consumeEvent(record2, mock(Acknowledgment.class));

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(createdCount.get()).isEqualTo(1);
        assertThat(confirmedCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Debe manejar eventos con sequence numbers correctos")
    void testEventSequenceNumbers() throws Exception {
        // Given
        String aggregateId = "ORDER-010";
        CountDownLatch latch = new CountDownLatch(5);
        AtomicInteger sequenceSum = new AtomicInteger(0);

        eventBus.registerEventHandler(evt -> {
            sequenceSum.addAndGet((int) evt.getSequenceNumber());
            latch.countDown();
        });

        // When - Publicar eventos en secuencia
        for (long seq = 0; seq < 5; seq++) {
            OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-" + seq, 100.0);
            DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
                "Order", aggregateId, seq, event, MetaData.emptyInstance()
            );
            
            String serialized = eventSerializer.serialize(eventMessage);
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "test-events", 0, (int) seq, aggregateId, serialized
            );
            
            eventBus.consumeEvent(record, mock(Acknowledgment.class));
        }

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(sequenceSum.get()).isEqualTo(0 + 1 + 2 + 3 + 4); // Suma de secuencias
    }

    @Test
    @DisplayName("Debe manejar errores de deserialización")
    void testHandleDeserializationError() {
        // Given
        String invalidJson = "{invalid json}";
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-events", 0, 0, "key", invalidJson
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When / Then - No debería lanzar excepción
        assertThatCode(() -> eventBus.consumeEvent(record, ack))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Debe usar routing key correcto al publicar")
    void testCorrectRoutingKey() {
        // Given
        String aggregateId = "ORDER-011";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-777", 600.00);
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );

        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(null));

        // When
        eventBus.publish(eventMessage);

        // Then - El routing key debe ser el aggregateId
        verify(kafkaTemplate).send(
            anyString(),
            eq(aggregateId), // Routing key
            anyString()
        );
    }

    @Test
    @DisplayName("Debe registrar handler de forma thread-safe")
    void testThreadSafeHandlerRegistration() throws InterruptedException {
        // Given
        int threadCount = 50;
        CountDownLatch latch = new CountDownLatch(threadCount);

        // When - Registrar handlers concurrentemente
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    eventBus.registerEventHandler(evt -> {});
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(5, TimeUnit.SECONDS);

        // Then - No debe lanzar ConcurrentModificationException
        // La lista interna usa CopyOnWriteArrayList que es thread-safe
    }
}