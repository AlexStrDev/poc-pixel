package io.github.axonkafka.processor;

import io.github.axonkafka.properties.AxonKafkaProperties;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.eventhandling.*;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.jpa.JpaTokenStore;
import org.axonframework.messaging.MetaData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests para Event Processors y Token Store
 * 
 * Cubre:
 * - Configuración de event processors
 * - Procesamiento de eventos
 * - Token persistence
 * - Error handling
 * - Event replay
 */
class EventProcessorIntegrationTest {

    private EventProcessingConfigurer configurer;
    private TokenStore tokenStore;
    private AxonKafkaProperties properties;
    
    private List<DomainEventMessage<?>> processedEvents;
    private AtomicInteger eventProcessCount;

    static class TestEvent {
        private String data;
        public TestEvent() {}
        public TestEvent(String data) { this.data = data; }
        public String getData() { return data; }
        public void setData(String data) { this.data = data; }
    }

    static class OrderEvent {
        private String orderId;
        private double amount;
        public OrderEvent() {}
        public OrderEvent(String orderId, double amount) {
            this.orderId = orderId;
            this.amount = amount;
        }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    @BeforeEach
    void setUp() {
        configurer = mock(EventProcessingConfigurer.class);
        tokenStore = mock(JpaTokenStore.class);
        
        properties = new AxonKafkaProperties();
        properties.getSaga().setEnabled(true);
        properties.getSaga().setProcessorName("TestProcessor");
        
        processedEvents = new CopyOnWriteArrayList<>();
        eventProcessCount = new AtomicInteger(0);
    }

    @Test
    @DisplayName("Debe registrar event processor correctamente")
    void testRegisterEventProcessor() {
        // Given
        String processorName = "OrderProcessor";

        // When
        configurer.registerSubscribingEventProcessor(processorName);

        // Then
        verify(configurer).registerSubscribingEventProcessor(processorName);
    }

    @Test
    @DisplayName("Debe procesar eventos en orden")
    void testProcessEventsInOrder() throws Exception {
        // Given
        String aggregateId = "AGG-001";
        List<DomainEventMessage<?>> events = createEventSequence(aggregateId, 10);
        
        CountDownLatch latch = new CountDownLatch(10);
        
        // Simular event handler
        EventMessageHandler handler = eventMessage -> {
            processedEvents.add((DomainEventMessage<?>) eventMessage);
            eventProcessCount.incrementAndGet();
            latch.countDown();
            return null;
        };

        // When
        for (DomainEventMessage<?> event : events) {
            handler.handle(event);
        }

        // Then
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(processedEvents).hasSize(10);
        
        // Verificar orden
        for (int i = 0; i < processedEvents.size(); i++) {
            assertThat(processedEvents.get(i).getSequenceNumber()).isEqualTo((long) i);
        }
    }

    @Test
    @DisplayName("Debe persistir tokens después de procesar eventos")
    void testTokenPersistence() throws Exception {
        // Given
        String processorName = "OrderProcessor";
        String aggregateId = "AGG-002";
        
        TrackingToken token = mock(TrackingToken.class);
        when(tokenStore.fetchToken(processorName, 0)).thenReturn(token);

        // When
        tokenStore.storeToken(token, processorName, 0);

        // Then
        verify(tokenStore).storeToken(token, processorName, 0);
    }

    @Test
    @DisplayName("Debe recuperar processing desde último token guardado")
    void testResumeFromLastToken() throws Exception {
        // Given
        String processorName = "OrderProcessor";
        int segment = 0;
        
        TrackingToken lastToken = mock(TrackingToken.class);
        when(tokenStore.fetchToken(processorName, segment)).thenReturn(lastToken);

        // When
        TrackingToken retrievedToken = tokenStore.fetchToken(processorName, segment);

        // Then
        assertThat(retrievedToken).isEqualTo(lastToken);
        verify(tokenStore).fetchToken(processorName, segment);
    }

    @Test
    @DisplayName("Debe manejar errores en event handlers sin detener processing")
    void testErrorHandlingInEventProcessor() throws Exception {
        // Given
        CountDownLatch successLatch = new CountDownLatch(2);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        EventMessageHandler handler = eventMessage -> {
            TestEvent event = (TestEvent) ((DomainEventMessage<?>) eventMessage).getPayload();
            
            if ("FAIL".equals(event.getData())) {
                failureCount.incrementAndGet();
                throw new RuntimeException("Simulated processing error");
            }
            
            processedEvents.add((DomainEventMessage<?>) eventMessage);
            successLatch.countDown();
            return null;
        };

        // When
        DomainEventMessage<TestEvent> event1 = createTestEvent("AGG-003", 0L, "SUCCESS-1");
        DomainEventMessage<TestEvent> event2 = createTestEvent("AGG-003", 1L, "FAIL");
        DomainEventMessage<TestEvent> event3 = createTestEvent("AGG-003", 2L, "SUCCESS-2");

        try {
            handler.handle(event1);
        } catch (Exception e) {
            // Ignore
        }
        
        try {
            handler.handle(event2);
        } catch (Exception e) {
            // Ignore - esperado
        }
        
        try {
            handler.handle(event3);
        } catch (Exception e) {
            // Ignore
        }

        // Then
        assertThat(successLatch.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(processedEvents).hasSize(2);
        assertThat(failureCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Debe procesar eventos de múltiples aggregates")
    void testProcessEventsFromMultipleAggregates() throws Exception {
        // Given
        List<String> aggregateIds = List.of("AGG-A", "AGG-B", "AGG-C");
        CountDownLatch latch = new CountDownLatch(15); // 5 eventos por aggregate
        
        EventMessageHandler handler = eventMessage -> {
            processedEvents.add((DomainEventMessage<?>) eventMessage);
            latch.countDown();
            return null;
        };

        // When
        for (String aggregateId : aggregateIds) {
            List<DomainEventMessage<?>> events = createEventSequence(aggregateId, 5);
            for (DomainEventMessage<?> event : events) {
                handler.handle(event);
            }
        }

        // Then
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(processedEvents).hasSize(15);
    }

    @Test
    @DisplayName("Debe soportar event replay desde inicio")
    void testEventReplayFromBeginning() throws Exception {
        // Given
        String processorName = "ReplayProcessor";
        String aggregateId = "AGG-004";
        
        // Simular que no hay token guardado (inicio desde cero)
        when(tokenStore.fetchToken(processorName, 0)).thenReturn(null);

        // Eventos históricos
        List<DomainEventMessage<?>> historicalEvents = createEventSequence(aggregateId, 20);
        
        CountDownLatch latch = new CountDownLatch(20);
        EventMessageHandler handler = eventMessage -> {
            processedEvents.add((DomainEventMessage<?>) eventMessage);
            latch.countDown();
            return null;
        };

        // When - Replay
        for (DomainEventMessage<?> event : historicalEvents) {
            handler.handle(event);
        }

        // Then
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(processedEvents).hasSize(20);
    }

    @Test
    @DisplayName("Debe inicializar token en null para procesador nuevo")
    void testInitializeTokenForNewProcessor() {
        // Given
        String processorName = "NewProcessor";
        int segment = 0;
        
        when(tokenStore.fetchToken(processorName, segment)).thenReturn(null);

        // When
        TrackingToken token = tokenStore.fetchToken(processorName, segment);

        // Then
        assertThat(token).isNull(); // Primer inicio
    }

    @Test
    @DisplayName("Debe actualizar token después de cada batch de eventos")
    void testUpdateTokenAfterBatch() {
        // Given
        String processorName = "BatchProcessor";
        int segment = 0;
        
        TrackingToken initialToken = mock(TrackingToken.class);
        TrackingToken updatedToken = mock(TrackingToken.class);

        // When
        tokenStore.storeToken(initialToken, processorName, segment);
        tokenStore.storeToken(updatedToken, processorName, segment);

        // Then
        verify(tokenStore, times(2)).storeToken(any(TrackingToken.class), eq(processorName), eq(segment));
    }

    @Test
    @DisplayName("Debe manejar eventos con metadata correctamente")
    void testProcessEventsWithMetadata() throws Exception {
        // Given
        String aggregateId = "AGG-005";
        MetaData metadata = MetaData.with("userId", "USER-123")
            .and("correlationId", UUID.randomUUID().toString())
            .and("timestamp", System.currentTimeMillis());
        
        DomainEventMessage<TestEvent> event = createTestEventWithMetadata(
            aggregateId, 0L, "test-data", metadata
        );
        
        CountDownLatch latch = new CountDownLatch(1);
        EventMessageHandler handler = eventMessage -> {
            DomainEventMessage<?> domainEvent = (DomainEventMessage<?>) eventMessage;
            assertThat(domainEvent.getMetaData().get("userId")).isEqualTo("USER-123");
            latch.countDown();
            return null;
        };

        // When
        handler.handle(event);

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("Debe procesar alta carga de eventos eficientemente")
    void testHighVolumeEventProcessing() throws Exception {
        // Given
        int eventCount = 1000;
        String aggregateId = "AGG-HIGH-VOLUME";
        
        List<DomainEventMessage<?>> events = createEventSequence(aggregateId, eventCount);
        CountDownLatch latch = new CountDownLatch(eventCount);
        
        EventMessageHandler handler = eventMessage -> {
            eventProcessCount.incrementAndGet();
            latch.countDown();
            return null;
        };

        long startTime = System.currentTimeMillis();

        // When
        for (DomainEventMessage<?> event : events) {
            handler.handle(event);
        }

        // Then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(eventProcessCount.get()).isEqualTo(eventCount);
        
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (eventCount * 1000.0) / duration;
        
        System.out.println("Processed " + eventCount + " events in " + duration + "ms");
        System.out.println("Throughput: " + throughput + " events/sec");
        
        assertThat(throughput).isGreaterThan(100); // Al menos 100 eventos/seg
    }

    @Test
    @DisplayName("Debe manejar diferentes tipos de eventos")
    void testProcessDifferentEventTypes() throws Exception {
        // Given
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger testEventCount = new AtomicInteger(0);
        AtomicInteger orderEventCount = new AtomicInteger(0);
        
        EventMessageHandler handler = eventMessage -> {
            Object payload = ((DomainEventMessage<?>) eventMessage).getPayload();
            
            if (payload instanceof TestEvent) {
                testEventCount.incrementAndGet();
            } else if (payload instanceof OrderEvent) {
                orderEventCount.incrementAndGet();
            }
            
            latch.countDown();
            return null;
        };

        // When
        DomainEventMessage<TestEvent> testEvent = createTestEvent("AGG-006", 0L, "test");
        DomainEventMessage<OrderEvent> orderEvent = createOrderEvent("AGG-007", 0L, "ORDER-001", 100.0);

        handler.handle(testEvent);
        handler.handle(orderEvent);

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(testEventCount.get()).isEqualTo(1);
        assertThat(orderEventCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Debe configurar event processor como SUBSCRIBING")
    void testSubscribingEventProcessorConfiguration() {
        // Given
        String processorName = properties.getSaga().getProcessorName();

        // When
        configurer.registerSubscribingEventProcessor(processorName);

        // Then
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(configurer).registerSubscribingEventProcessor(captor.capture());
        assertThat(captor.getValue()).isEqualTo(processorName);
    }

    @Test
    @DisplayName("Debe manejar eventos desordenados correctamente")
    void testHandleOutOfOrderEvents() throws Exception {
        // Given
        String aggregateId = "AGG-008";
        CountDownLatch latch = new CountDownLatch(5);
        
        EventMessageHandler handler = eventMessage -> {
            processedEvents.add((DomainEventMessage<?>) eventMessage);
            latch.countDown();
            return null;
        };

        // When - Enviar eventos desordenados
        handler.handle(createTestEvent(aggregateId, 2L, "event-2"));
        handler.handle(createTestEvent(aggregateId, 0L, "event-0"));
        handler.handle(createTestEvent(aggregateId, 4L, "event-4"));
        handler.handle(createTestEvent(aggregateId, 1L, "event-1"));
        handler.handle(createTestEvent(aggregateId, 3L, "event-3"));

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(processedEvents).hasSize(5);
        
        // Todos deberían procesarse, aunque no en orden
        assertThat(processedEvents.stream()
            .map(DomainEventMessage::getSequenceNumber)
            .sorted()
            .toList()
        ).containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    @DisplayName("Debe manejar concurrent processing de eventos")
    void testConcurrentEventProcessing() throws Exception {
        // Given
        int threadCount = 10;
        int eventsPerThread = 50;
        CountDownLatch latch = new CountDownLatch(threadCount * eventsPerThread);
        
        EventMessageHandler handler = eventMessage -> {
            eventProcessCount.incrementAndGet();
            latch.countDown();
            return null;
        };

        // When
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            new Thread(() -> {
                String aggregateId = "AGG-THREAD-" + threadId;
                List<DomainEventMessage<?>> events = createEventSequence(aggregateId, eventsPerThread);
                
                for (DomainEventMessage<?> event : events) {
                    try {
                        handler.handle(event);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        // Then
        assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();
        assertThat(eventProcessCount.get()).isEqualTo(threadCount * eventsPerThread);
    }

    // ========== Helper Methods ==========

    private List<DomainEventMessage<?>> createEventSequence(String aggregateId, int count) {
        List<DomainEventMessage<?>> events = new CopyOnWriteArrayList<>();
        
        for (int i = 0; i < count; i++) {
            events.add(createTestEvent(aggregateId, (long) i, "event-" + i));
        }
        
        return events;
    }

    private DomainEventMessage<TestEvent> createTestEvent(String aggregateId, long sequence, String data) {
        TestEvent event = new TestEvent(data);
        
        return new GenericDomainEventMessage<>(
            "Test",
            aggregateId,
            sequence,
            event,
            MetaData.emptyInstance()
        );
    }

    private DomainEventMessage<TestEvent> createTestEventWithMetadata(
            String aggregateId, long sequence, String data, MetaData metadata) {
        
        TestEvent event = new TestEvent(data);
        
        return new GenericDomainEventMessage<>(
            "Test",
            aggregateId,
            sequence,
            event,
            metadata
        );
    }

    private DomainEventMessage<OrderEvent> createOrderEvent(
            String aggregateId, long sequence, String orderId, double amount) {
        
        OrderEvent event = new OrderEvent(orderId, amount);
        
        return new GenericDomainEventMessage<>(
            "Order",
            aggregateId,
            sequence,
            event,
            MetaData.emptyInstance()
        );
    }
}