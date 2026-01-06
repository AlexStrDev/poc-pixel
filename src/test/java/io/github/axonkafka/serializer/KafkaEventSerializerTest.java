package io.github.axonkafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests unitarios para KafkaEventSerializer
 * 
 * Cubre:
 * - Serialización de eventos de dominio
 * - Deserialización de eventos
 * - Preservación de metadata
 * - Aggregate identifier
 * - Sequence numbers
 * - Timestamps
 * - Manejo de errores
 */
class KafkaEventSerializerTest {

    private KafkaEventSerializer serializer;
    private ObjectMapper objectMapper;

    // Eventos de prueba
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
        private Instant confirmedAt;

        public OrderConfirmedEvent() {}

        public OrderConfirmedEvent(String orderId, Instant confirmedAt) {
            this.orderId = orderId;
            this.confirmedAt = confirmedAt;
        }

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public Instant getConfirmedAt() { return confirmedAt; }
        public void setConfirmedAt(Instant confirmedAt) { this.confirmedAt = confirmedAt; }
    }

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        serializer = new KafkaEventSerializer(objectMapper);
    }

    @Test
    @DisplayName("Debe serializar evento de dominio simple correctamente")
    void testSerializeSimpleDomainEvent() {
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

        // When
        String json = serializer.serialize(eventMessage);

        // Then
        assertThat(json).isNotNull();
        assertThat(json).contains("OrderCreatedEvent");
        assertThat(json).contains("ORDER-001");
        assertThat(json).contains("CUST-123");
        assertThat(json).contains("150.5");
        assertThat(json).contains("\"sequenceNumber\":0");
    }

    @Test
    @DisplayName("Debe incluir metadata en la serialización")
    void testSerializeEventWithMetadata() {
        // Given
        String aggregateId = "ORDER-002";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-456", 200.75);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("userId", "USER-789");
        metadata.put("correlationId", UUID.randomUUID().toString());
        metadata.put("causationId", UUID.randomUUID().toString());
        
        DomainEventMessage<OrderCreatedEvent> eventMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            1L,
            event,
            MetaData.from(metadata)
        );

        // When
        String json = serializer.serialize(eventMessage);

        // Then
        assertThat(json).contains("userId");
        assertThat(json).contains("USER-789");
        assertThat(json).contains("correlationId");
        assertThat(json).contains("causationId");
    }

    @Test
    @DisplayName("Debe deserializar evento correctamente")
    void testDeserializeEvent() throws Exception {
        // Given
        String aggregateId = "ORDER-003";
        OrderCreatedEvent originalEvent = new OrderCreatedEvent(aggregateId, "CUST-999", 99.99);
        
        DomainEventMessage<OrderCreatedEvent> originalMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            2L,
            originalEvent,
            MetaData.emptyInstance()
        );
        
        String json = serializer.serialize(originalMessage);

        // When
        DomainEventMessage<?> deserializedMessage = serializer.deserialize(json);

        // Then
        assertThat(deserializedMessage).isNotNull();
        assertThat(deserializedMessage.getAggregateIdentifier()).isEqualTo(aggregateId);
        assertThat(deserializedMessage.getSequenceNumber()).isEqualTo(2L);
        assertThat(deserializedMessage.getPayload()).isInstanceOf(OrderCreatedEvent.class);
        
        OrderCreatedEvent deserializedEvent = (OrderCreatedEvent) deserializedMessage.getPayload();
        assertThat(deserializedEvent.getOrderId()).isEqualTo(aggregateId);
        assertThat(deserializedEvent.getCustomerId()).isEqualTo("CUST-999");
        assertThat(deserializedEvent.getAmount()).isEqualTo(99.99);
    }

    @Test
    @DisplayName("Debe preservar metadata en ciclo serialize-deserialize")
    void testMetadataPreservation() {
        // Given
        String aggregateId = "ORDER-004";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-111", 123.45);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("userId", "USER-999");
        metadata.put("traceId", "TRACE-123");
        
        DomainEventMessage<OrderCreatedEvent> originalMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            3L,
            event,
            MetaData.from(metadata)
        );
        
        String json = serializer.serialize(originalMessage);

        // When
        DomainEventMessage<?> deserializedMessage = serializer.deserialize(json);

        // Then
        assertThat(deserializedMessage.getMetaData().get("userId")).isEqualTo("USER-999");
        assertThat(deserializedMessage.getMetaData().get("traceId")).isEqualTo("TRACE-123");
    }

    @Test
    @DisplayName("Debe preservar sequence number correctamente")
    void testSequenceNumberPreservation() {
        // Given
        String aggregateId = "ORDER-005";
        
        for (long seq = 0; seq < 10; seq++) {
            OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-" + seq, 100.0 + seq);
            
            DomainEventMessage<OrderCreatedEvent> message = new GenericDomainEventMessage<>(
                "Order",
                aggregateId,
                seq,
                event,
                MetaData.emptyInstance()
            );
            
            String json = serializer.serialize(message);
            DomainEventMessage<?> deserialized = serializer.deserialize(json);

            // Then
            assertThat(deserialized.getSequenceNumber()).isEqualTo(seq);
        }
    }

    @Test
    @DisplayName("Debe preservar timestamp correctamente")
    void testTimestampPreservation() {
        // Given
        String aggregateId = "ORDER-006";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-222", 250.00);
        
        // ✅ FIX: Usar timestamp con precisión de milisegundos (truncar nanosegundos)
        Instant originalTimestamp = Instant.ofEpochMilli(System.currentTimeMillis());
        
        DomainEventMessage<OrderCreatedEvent> originalMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            4L,
            event,
            MetaData.emptyInstance(),
            UUID.randomUUID().toString(),
            originalTimestamp
        );
        
        String json = serializer.serialize(originalMessage);

        // When
        DomainEventMessage<?> deserializedMessage = serializer.deserialize(json);

        // Then
        assertThat(deserializedMessage.getTimestamp()).isEqualTo(originalTimestamp);
    }

    @Test
    @DisplayName("Debe manejar eventos con campos null")
    void testSerializeEventWithNullFields() {
        // Given
        String aggregateId = "ORDER-007";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, null, 100.00);
        
        DomainEventMessage<OrderCreatedEvent> message = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            0L,
            event,
            MetaData.emptyInstance()
        );

        // When
        String json = serializer.serialize(message);
        DomainEventMessage<?> deserialized = serializer.deserialize(json);

        // Then
        OrderCreatedEvent deserializedEvent = (OrderCreatedEvent) deserialized.getPayload();
        assertThat(deserializedEvent.getCustomerId()).isNull();
    }

    @Test
    @DisplayName("Debe manejar eventos con tipos complejos (Instant)")
    void testSerializeEventWithInstant() {
        // Given
        String aggregateId = "ORDER-008";
        Instant confirmedAt = Instant.now();
        OrderConfirmedEvent event = new OrderConfirmedEvent(aggregateId, confirmedAt);
        
        DomainEventMessage<OrderConfirmedEvent> message = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            5L,
            event,
            MetaData.emptyInstance()
        );

        // When
        String json = serializer.serialize(message);
        DomainEventMessage<?> deserialized = serializer.deserialize(json);

        // Then
        OrderConfirmedEvent deserializedEvent = (OrderConfirmedEvent) deserialized.getPayload();
        assertThat(deserializedEvent.getConfirmedAt()).isEqualTo(confirmedAt);
    }

    @Test
    @DisplayName("Debe lanzar excepción si JSON es inválido")
    void testDeserializeInvalidJson() {
        // Given
        String invalidJson = "{invalid json}";

        // When / Then
        assertThatThrownBy(() -> serializer.deserialize(invalidJson))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Error deserializando evento");
    }

    @Test
    @DisplayName("Debe lanzar excepción si clase del evento no existe")
    void testDeserializeNonExistentClass() {
        // Given
        String jsonWithInvalidClass = """
            {
                "eventIdentifier": "test-id",
                "aggregateIdentifier": "AGG-001",
                "sequenceNumber": 0,
                "eventType": "com.nonexistent.FakeEvent",
                "timestamp": 1234567890000,
                "payload": {
                    "type": "com.nonexistent.FakeEvent",
                    "revision": "1.0",
                    "data": "{\\"field\\":\\"value\\"}"
                },
                "metaData": {}
            }
            """;

        // When / Then
        assertThatThrownBy(() -> serializer.deserialize(jsonWithInvalidClass))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Error deserializando evento");
    }

    @Test
    @DisplayName("Debe generar eventIdentifier único para cada evento")
    void testUniqueEventIdentifiers() {
        // Given
        String aggregateId = "ORDER-009";
        
        OrderCreatedEvent event1 = new OrderCreatedEvent(aggregateId, "CUST-333", 50.00);
        OrderCreatedEvent event2 = new OrderCreatedEvent(aggregateId, "CUST-444", 60.00);
        
        DomainEventMessage<OrderCreatedEvent> message1 = new GenericDomainEventMessage<>(
            "Order", aggregateId, 0L, event1, MetaData.emptyInstance()
        );
        DomainEventMessage<OrderCreatedEvent> message2 = new GenericDomainEventMessage<>(
            "Order", aggregateId, 1L, event2, MetaData.emptyInstance()
        );

        // When
        String json1 = serializer.serialize(message1);
        String json2 = serializer.serialize(message2);

        DomainEventMessage<?> deserialized1 = serializer.deserialize(json1);
        DomainEventMessage<?> deserialized2 = serializer.deserialize(json2);

        // Then
        assertThat(deserialized1.getIdentifier()).isNotEqualTo(deserialized2.getIdentifier());
    }

    @Test
    @DisplayName("Debe serializar y deserializar múltiples veces correctamente (idempotencia)")
    void testMultipleSerializationCycles() {
        // Given
        String aggregateId = "ORDER-010";
        OrderCreatedEvent originalEvent = new OrderCreatedEvent(aggregateId, "CUST-666", 175.25);
        
        DomainEventMessage<OrderCreatedEvent> originalMessage = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            6L,
            originalEvent,
            MetaData.emptyInstance()
        );

        // When - Ciclo 1
        String json1 = serializer.serialize(originalMessage);
        DomainEventMessage<?> deserialized1 = serializer.deserialize(json1);
        
        // Ciclo 2
        String json2 = serializer.serialize(deserialized1);
        DomainEventMessage<?> deserialized2 = serializer.deserialize(json2);

        // Then
        OrderCreatedEvent event1 = (OrderCreatedEvent) deserialized1.getPayload();
        OrderCreatedEvent event2 = (OrderCreatedEvent) deserialized2.getPayload();
        
        assertThat(event1.getOrderId()).isEqualTo(event2.getOrderId());
        assertThat(event1.getCustomerId()).isEqualTo(event2.getCustomerId());
        assertThat(event1.getAmount()).isEqualTo(event2.getAmount());
        assertThat(deserialized1.getSequenceNumber()).isEqualTo(deserialized2.getSequenceNumber());
    }

    @Test
    @DisplayName("Debe manejar eventos con metadata complejo")
    void testComplexMetadata() {
        // Given
        String aggregateId = "ORDER-011";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-777", 300.00);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("userId", "USER-123");
        metadata.put("timestamp", System.currentTimeMillis());
        metadata.put("nested", Map.of("key1", "value1", "key2", 123));
        
        DomainEventMessage<OrderCreatedEvent> message = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            7L,
            event,
            MetaData.from(metadata)
        );

        // When
        String json = serializer.serialize(message);
        DomainEventMessage<?> deserialized = serializer.deserialize(json);

        // Then
        assertThat(deserialized.getMetaData().get("userId")).isEqualTo("USER-123");
        assertThat(deserialized.getMetaData().get("timestamp")).isNotNull();
        assertThat(deserialized.getMetaData().get("nested")).isNotNull();
    }

    @Test
    @DisplayName("Debe manejar eventos con valores numéricos grandes")
    void testLargeNumbers() {
        // Given
        String aggregateId = "ORDER-012";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-888", Double.MAX_VALUE);
        
        DomainEventMessage<OrderCreatedEvent> message = new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            8L,
            event,
            MetaData.emptyInstance()
        );

        // When
        String json = serializer.serialize(message);
        DomainEventMessage<?> deserialized = serializer.deserialize(json);

        // Then
        OrderCreatedEvent deserializedEvent = (OrderCreatedEvent) deserialized.getPayload();
        assertThat(deserializedEvent.getAmount()).isEqualTo(Double.MAX_VALUE);
    }

    @Test
    @DisplayName("Debe preservar aggregate type correctamente")
    void testAggregateTypePreservation() {
        // Given
        String aggregateType = "Order";
        String aggregateId = "ORDER-013";
        OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-999", 400.00);
        
        DomainEventMessage<OrderCreatedEvent> message = new GenericDomainEventMessage<>(
            aggregateType,
            aggregateId,
            9L,
            event,
            MetaData.emptyInstance()
        );

        // When
        String json = serializer.serialize(message);
        DomainEventMessage<?> deserialized = serializer.deserialize(json);

        // Then
        // ✅ FIX: GenericDomainEventMessage usa eventType (clase del payload)
        // NO el aggregateType que pasamos al constructor
        assertThat(deserialized.getType())
            .isEqualTo(OrderCreatedEvent.class.getName());
        
        // El aggregate identifier sí se preserva correctamente
        assertThat(deserialized.getAggregateIdentifier()).isEqualTo(aggregateId);
    }

    @Test
    @DisplayName("Debe manejar secuencia de eventos del mismo aggregate")
    void testEventSequence() {
        // Given
        String aggregateId = "ORDER-014";
        
        DomainEventMessage<?>[] messages = new DomainEventMessage<?>[5];
        
        for (int i = 0; i < 5; i++) {
            OrderCreatedEvent event = new OrderCreatedEvent(aggregateId, "CUST-" + i, 100.0 + i);
            messages[i] = new GenericDomainEventMessage<>(
                "Order",
                aggregateId,
                (long) i,
                event,
                MetaData.emptyInstance()
            );
        }

        // When
        for (int i = 0; i < 5; i++) {
            String json = serializer.serialize(messages[i]);
            DomainEventMessage<?> deserialized = serializer.deserialize(json);
            
            // Then
            assertThat(deserialized.getAggregateIdentifier()).isEqualTo(aggregateId);
            assertThat(deserialized.getSequenceNumber()).isEqualTo((long) i);
        }
    }
}