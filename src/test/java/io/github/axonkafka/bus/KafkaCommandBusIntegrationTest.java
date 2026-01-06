package io.github.axonkafka.bus;

import io.github.axonkafka.cache.CommandDeduplicationService;
import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.CommandSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.modelling.command.TargetAggregateIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests de integración para KafkaCommandBus
 * 
 * Cubre:
 * - Consumo de comandos desde Kafka
 * - Dispatch a handlers locales
 * - Idempotencia
 * - DLQ para errores
 * - Request-Reply pattern
 * - Métricas
 */
class KafkaCommandBusIntegrationTest {

    private KafkaCommandBus kafkaCommandBus;
    private CommandBus localCommandBus;
    private CommandSerializer commandSerializer;
    private CommandDeduplicationService deduplicationService;
    private KafkaTemplate<String, String> kafkaTemplate;
    private AxonKafkaProperties properties;

    // Comando de prueba
    static class TestCommand {
        @TargetAggregateIdentifier
        private String aggregateId;
        private String data;

        public TestCommand() {}
        public TestCommand(String aggregateId, String data) {
            this.aggregateId = aggregateId;
            this.data = data;
        }

        public String getAggregateId() { return aggregateId; }
        public void setAggregateId(String aggregateId) { this.aggregateId = aggregateId; }
        public String getData() { return data; }
        public void setData(String data) { this.data = data; }
    }

    @BeforeEach
    void setUp() {
        localCommandBus = SimpleCommandBus.builder().build();
        commandSerializer = new CommandSerializer();
        deduplicationService = new CommandDeduplicationService();
        kafkaTemplate = mock(KafkaTemplate.class);
        
        properties = new AxonKafkaProperties();
        properties.getCommand().setTopic("test-commands");
        properties.getCommand().setReplyTopic("test-command-replies");
        properties.getCommand().setDlqTopic("test-commands-dlq");

        kafkaCommandBus = new KafkaCommandBus(
            localCommandBus,
            commandSerializer,
            deduplicationService,
            kafkaTemplate,
            properties
        );
    }

    @Test
    @DisplayName("Debe registrar handler y procesar comando correctamente")
    void testRegisterHandlerAndProcessCommand() throws Exception {
        // Given
        String commandId = UUID.randomUUID().toString();
        TestCommand command = new TestCommand("AGG-001", "test-data");
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(Map.of("messageIdentifier", commandId));
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<CommandMessage<?>> receivedCommand = new AtomicReference<>();

        MessageHandler<CommandMessage<?>> handler = msg -> {
            receivedCommand.set(msg);
            latch.countDown();
            return null;
        };

        // When
        kafkaCommandBus.subscribe(TestCommand.class.getName(), handler);
        
        String serializedCommand = commandSerializer.serialize(commandMessage, "AGG-001");
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-commands", 0, 0, "AGG-001", serializedCommand
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        kafkaCommandBus.consumeCommand(record, ack);

        // Then
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(receivedCommand.get()).isNotNull();
        assertThat(receivedCommand.get().getPayload()).isInstanceOf(TestCommand.class);
        
        TestCommand processedCommand = (TestCommand) receivedCommand.get().getPayload();
        assertThat(processedCommand.getAggregateId()).isEqualTo("AGG-001");
        assertThat(processedCommand.getData()).isEqualTo("test-data");
        
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Debe bloquear comandos duplicados")
    void testRejectDuplicateCommands() throws Exception {
        // Given
        String messageId = "MSG-001";
        TestCommand command = new TestCommand("AGG-002", "duplicate-test");
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(Map.of("messageIdentifier", messageId));

        AtomicInteger handlerCallCount = new AtomicInteger(0);
        MessageHandler<CommandMessage<?>> handler = msg -> {
            handlerCallCount.incrementAndGet();
            return null;
        };

        kafkaCommandBus.subscribe(TestCommand.class.getName(), handler);

        String serializedCommand = commandSerializer.serialize(commandMessage, "AGG-002");
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-commands", 0, 0, "AGG-002", serializedCommand
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When - Primera vez
        kafkaCommandBus.consumeCommand(record, ack);
        Thread.sleep(100);

        // Segunda vez (duplicado)
        kafkaCommandBus.consumeCommand(record, ack);
        Thread.sleep(100);

        // Then
        assertThat(handlerCallCount.get()).isEqualTo(1); // Solo procesado una vez
        verify(ack, times(2)).acknowledge(); // Ambos reconocidos
    }

    @Test
    @DisplayName("Debe enviar comandos fallidos a DLQ")
    void testSendFailedCommandsToDLQ() throws Exception {
        // Given
        String messageId = "MSG-002";
        TestCommand command = new TestCommand("AGG-003", "fail-test");
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(Map.of("messageIdentifier", messageId));

        MessageHandler<CommandMessage<?>> failingHandler = msg -> {
            throw new KafkaCommandBus.BusinessException("Business rule violation");
        };

        kafkaCommandBus.subscribe(TestCommand.class.getName(), failingHandler);

        String serializedCommand = commandSerializer.serialize(commandMessage, "AGG-003");
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-commands", 0, 0, "AGG-003", serializedCommand
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        kafkaCommandBus.consumeCommand(record, ack);
        Thread.sleep(200);

        // Then
        verify(kafkaTemplate).send(
            eq("test-commands-dlq"),
            eq("AGG-003"),
            contains("Business rule violation")
        );
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Debe enviar respuesta exitosa")
    void testSendSuccessfulReply() throws Exception {
        // Given
        String messageId = "MSG-003";
        String correlationId = UUID.randomUUID().toString();
        TestCommand command = new TestCommand("AGG-004", "reply-test");
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(Map.of(
                "messageIdentifier", messageId,
                "correlationId", correlationId
            ));

        MessageHandler<CommandMessage<?>> handler = msg -> "Success Result";

        kafkaCommandBus.subscribe(TestCommand.class.getName(), handler);

        String serializedCommand = commandSerializer.serialize(commandMessage, "AGG-004");
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-commands", 0, 0, "AGG-004", serializedCommand
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        kafkaCommandBus.consumeCommand(record, ack);
        Thread.sleep(200);

        // Then
        verify(kafkaTemplate).send(
            eq("test-command-replies"),
            eq(correlationId),
            contains("\"success\":true")
        );
    }

    @Test
    @DisplayName("Debe enviar respuesta de error")
    void testSendErrorReply() throws Exception {
        // Given
        String messageId = "MSG-004";
        String correlationId = UUID.randomUUID().toString();
        TestCommand command = new TestCommand("AGG-005", "error-reply-test");
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(Map.of(
                "messageIdentifier", messageId,
                "correlationId", correlationId
            ));

        MessageHandler<CommandMessage<?>> failingHandler = msg -> {
            throw new RuntimeException("Technical error");
        };

        kafkaCommandBus.subscribe(TestCommand.class.getName(), failingHandler);

        String serializedCommand = commandSerializer.serialize(commandMessage, "AGG-005");
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-commands", 0, 0, "AGG-005", serializedCommand
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        kafkaCommandBus.consumeCommand(record, ack);
        Thread.sleep(200);

        // Then
        verify(kafkaTemplate).send(
            eq("test-command-replies"),
            eq(correlationId),
            contains("\"success\":false")
        );
    }

    @Test
    @DisplayName("Debe actualizar métricas correctamente")
    void testUpdateMetrics() throws Exception {
        // Given
        TestCommand command1 = new TestCommand("AGG-006", "metrics-test-1");
        TestCommand command2 = new TestCommand("AGG-007", "metrics-test-2");
        
        MessageHandler<CommandMessage<?>> handler = msg -> null;
        kafkaCommandBus.subscribe(TestCommand.class.getName(), handler);

        // When - Procesar comandos exitosos
        processCommand(command1, "MSG-005");
        processCommand(command2, "MSG-006");
        Thread.sleep(200);

        // Then
        Map<String, Object> metrics = kafkaCommandBus.getMetrics();
        assertThat(metrics.get("processedCommands")).isEqualTo(2L);
        assertThat(metrics.get("failedCommands")).isEqualTo(0L);
    }

    @Test
    @DisplayName("Debe manejar múltiples handlers concurrentemente")
    void testConcurrentHandlers() throws Exception {
        // Given
        int commandCount = 50;
        CountDownLatch latch = new CountDownLatch(commandCount);
        AtomicInteger processedCount = new AtomicInteger(0);

        MessageHandler<CommandMessage<?>> handler = msg -> {
            processedCount.incrementAndGet();
            latch.countDown();
            return null;
        };

        kafkaCommandBus.subscribe(TestCommand.class.getName(), handler);

        // When
        for (int i = 0; i < commandCount; i++) {
            TestCommand command = new TestCommand("AGG-" + i, "concurrent-test");
            processCommand(command, "MSG-" + i);
        }

        // Then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(processedCount.get()).isEqualTo(commandCount);
    }

    @Test
    @DisplayName("Debe desregistrar handler correctamente")
    void testUnregisterHandler() throws Exception {
        // Given
        AtomicInteger callCount = new AtomicInteger(0);
        MessageHandler<CommandMessage<?>> handler = msg -> {
            callCount.incrementAndGet();
            return null;
        };

        var registration = kafkaCommandBus.subscribe(TestCommand.class.getName(), handler);

        // Procesar antes de desregistrar
        TestCommand command1 = new TestCommand("AGG-008", "before-unregister");
        processCommand(command1, "MSG-007");
        Thread.sleep(100);

        // When - Desregistrar
        registration.cancel();

        // Intentar procesar después de desregistrar
        TestCommand command2 = new TestCommand("AGG-009", "after-unregister");
        processCommand(command2, "MSG-008");
        Thread.sleep(100);

        // Then
        assertThat(callCount.get()).isEqualTo(1); // Solo el primero se procesó
    }

    @Test
    @DisplayName("Debe manejar comandos sin correlationId")
    void testCommandsWithoutCorrelationId() throws Exception {
        // Given
        String messageId = "MSG-009";
        TestCommand command = new TestCommand("AGG-010", "no-correlation-test");
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(Map.of("messageIdentifier", messageId));

        MessageHandler<CommandMessage<?>> handler = msg -> "OK";
        kafkaCommandBus.subscribe(TestCommand.class.getName(), handler);

        String serializedCommand = commandSerializer.serialize(commandMessage, "AGG-010");
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-commands", 0, 0, "AGG-010", serializedCommand
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        kafkaCommandBus.consumeCommand(record, ack);
        Thread.sleep(100);

        // Then
        verify(ack).acknowledge();
        // No debe enviar respuesta si no hay correlationId
        verify(kafkaTemplate, never()).send(
            eq("test-command-replies"),
            anyString(),
            anyString()
        );
    }

    // Helper method
    private void processCommand(TestCommand command, String messageId) throws Exception {
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(Map.of("messageIdentifier", messageId));
        
        String serializedCommand = commandSerializer.serialize(commandMessage, command.getAggregateId());
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-commands", 0, 0, command.getAggregateId(), serializedCommand
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        kafkaCommandBus.consumeCommand(record, ack);
    }
}