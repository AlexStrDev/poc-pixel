package io.github.axonkafka.gateway;

import io.github.axonkafka.handler.CommandReplyHandler;
import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.CommandSerializer;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.CommandResultMessage;
import org.axonframework.modelling.command.TargetAggregateIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests de integración para KafkaCommandGateway
 * 
 * Cubre:
 * - Envío de comandos a Kafka
 * - Request-Reply pattern
 * - Timeouts
 * - Manejo de errores
 * - Métricas
 */
class KafkaCommandGatewayIntegrationTest {

    private KafkaCommandGateway commandGateway;
    private KafkaTemplate<String, String> kafkaTemplate;
    private CommandSerializer commandSerializer;
    private CommandReplyHandler replyHandler;
    private AxonKafkaProperties properties;

    // Comando de prueba
    static class CreateOrderCommand {
        @TargetAggregateIdentifier
        private String orderId;
        private String customerId;
        private double amount;

        public CreateOrderCommand() {}
        public CreateOrderCommand(String orderId, String customerId, double amount) {
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

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        commandSerializer = new CommandSerializer();
        replyHandler = mock(CommandReplyHandler.class);
        
        properties = new AxonKafkaProperties();
        properties.getCommand().setTopic("test-commands");
        properties.getCommand().setReplyTopic("test-command-replies");
        properties.getCommand().setTimeoutSeconds(5);

        commandGateway = new KafkaCommandGateway(
            kafkaTemplate,
            commandSerializer,
            replyHandler,
            properties
        );
    }

    @Test
    @DisplayName("Debe enviar comando a Kafka correctamente")
    void testSendCommandToKafka() throws Exception {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-001", "CUST-123", 150.50);
        
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        CompletableFuture<CommandReplyHandler.CommandResult> replyFuture = 
            new CompletableFuture<>();
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(replyFuture);

        CountDownLatch latch = new CountDownLatch(1);

        // When
        commandGateway.send(command, new CommandCallback<Object, Object>() {
            @Override
            public void onResult(CommandMessage commandMessage, CommandResultMessage commandResultMessage) {
                latch.countDown();
            }
        });

        // Simular respuesta exitosa
        replyFuture.complete(CommandReplyHandler.CommandResult.success("test-correlation", "OK"));

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        verify(kafkaTemplate).send(
            eq("test-commands"),
            eq("ORDER-001"),
            anyString()
        );
        verify(replyHandler).registerPendingCommand(anyString());
    }

    @Test
    @DisplayName("Debe manejar error al enviar a Kafka")
    void testHandleKafkaSendError() throws Exception {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-002", "CUST-456", 200.75);
        
        CompletableFuture<SendResult<String, String>> sendFuture = new CompletableFuture<>();
        sendFuture.completeExceptionally(new RuntimeException("Kafka connection error"));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        CompletableFuture<CommandReplyHandler.CommandResult> replyFuture = 
            new CompletableFuture<>();
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(replyFuture);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        // When
        commandGateway.send(command, new CommandCallback<Object, Object>() {
            @Override
            public void onResult(CommandMessage commandMessage, CommandResultMessage commandResultMessage) {
                if (commandResultMessage.isExceptional()) {
                    capturedError.set(commandResultMessage.exceptionResult());
                }
                latch.countDown();
            }
        });

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedError.get()).isNotNull();
        assertThat(capturedError.get().getMessage()).contains("Kafka connection error");
    }

    @Test
    @DisplayName("Debe procesar respuesta exitosa del comando")
    void testProcessSuccessfulCommandReply() throws Exception {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-003", "CUST-789", 99.99);
        
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        CompletableFuture<CommandReplyHandler.CommandResult> replyFuture = 
            new CompletableFuture<>();
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(replyFuture);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Object> capturedResult = new AtomicReference<>();

        // When
        commandGateway.send(command, new CommandCallback<Object, Object>() {
            @Override
            public void onResult(CommandMessage commandMessage, CommandResultMessage commandResultMessage) {
                if (!commandResultMessage.isExceptional()) {
                    capturedResult.set(commandResultMessage.getPayload());
                }
                latch.countDown();
            }
        });

        // Simular respuesta exitosa
        CommandReplyHandler.CommandResult successResult = 
            CommandReplyHandler.CommandResult.success("test-correlation", "Order created successfully");
        replyFuture.complete(successResult);

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedResult.get()).isEqualTo("Order created successfully");
    }

    @Test
    @DisplayName("Debe procesar respuesta de error del comando")
    void testProcessErrorCommandReply() throws Exception {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-004", "CUST-999", 300.00);
        
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        CompletableFuture<CommandReplyHandler.CommandResult> replyFuture = 
            new CompletableFuture<>();
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(replyFuture);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> capturedError = new AtomicReference<>();

        // When
        commandGateway.send(command, new CommandCallback<Object, Object>() {
            @Override
            public void onResult(CommandMessage commandMessage, CommandResultMessage commandResultMessage) {
                if (commandResultMessage.isExceptional()) {
                    capturedError.set(commandResultMessage.exceptionResult());
                }
                latch.countDown();
            }
        });

        // Simular respuesta de error
        CommandReplyHandler.CommandResult errorResult = 
            CommandReplyHandler.CommandResult.error("test-correlation", "Insufficient funds");
        replyFuture.complete(errorResult);

        // Then
        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedError.get()).isInstanceOf(KafkaCommandGateway.CommandExecutionException.class);
        assertThat(capturedError.get().getMessage()).contains("Insufficient funds");
    }

    @Test
    @DisplayName("Debe enviar y esperar respuesta (sendAndWait)")
    void testSendAndWait() throws Exception {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-005", "CUST-111", 50.00);
        
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        CompletableFuture<CommandReplyHandler.CommandResult> replyFuture = 
            new CompletableFuture<>();
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(replyFuture);

        // Simular respuesta asíncrona
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(500);
                replyFuture.complete(
                    CommandReplyHandler.CommandResult.success("test-correlation", "ORDER-005")
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // When
        String result = commandGateway.sendAndWait(command);

        // Then
        assertThat(result).isEqualTo("ORDER-005");
    }

    @Test
    @DisplayName("Debe lanzar timeout en sendAndWait si no hay respuesta")
    void testSendAndWaitTimeout() {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-006", "CUST-222", 75.00);
        
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        // Reply future que nunca se completa
        CompletableFuture<CommandReplyHandler.CommandResult> replyFuture = 
            new CompletableFuture<>();
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(replyFuture);

        // When / Then
        assertThatThrownBy(() -> commandGateway.sendAndWait(command, 1, TimeUnit.SECONDS))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Error esperando respuesta");
    }

    @Test
    @DisplayName("Debe retornar CompletableFuture con send() async")
    void testSendAsync() throws Exception {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-007", "CUST-333", 125.00);
        
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        CompletableFuture<CommandReplyHandler.CommandResult> replyFuture = 
            new CompletableFuture<>();
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(replyFuture);

        // When
        CompletableFuture<String> resultFuture = commandGateway.send(command);

        // Simular respuesta
        replyFuture.complete(
            CommandReplyHandler.CommandResult.success("test-correlation", "Success")
        );

        // Then
        String result = resultFuture.get(2, TimeUnit.SECONDS);
        assertThat(result).isEqualTo("Success");
    }

    @Test
    @DisplayName("Debe actualizar métricas correctamente")
    void testMetricsTracking() throws Exception {
        // Given
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(new CompletableFuture<>());

        // When - Enviar múltiples comandos
        for (int i = 0; i < 5; i++) {
            CreateOrderCommand command = new CreateOrderCommand("ORDER-" + i, "CUST-" + i, 100.0 + i);
            commandGateway.send(command, (cmdMsg, resultMsg) -> {});
        }

        Thread.sleep(500);

        // Then
        Map<String, Object> metrics = commandGateway.getMetrics();
        assertThat(metrics.get("sentCommands")).isEqualTo(5L);
        assertThat(metrics.get("failedSends")).isEqualTo(0L);
    }

    @Test
    @DisplayName("Debe registrar fallas en métricas")
    void testFailureMetrics() throws Exception {
        // Given
        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Send failed"));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(failedFuture);

        CountDownLatch latch = new CountDownLatch(3);

        // When - Enviar comandos que fallarán
        for (int i = 0; i < 3; i++) {
            CreateOrderCommand command = new CreateOrderCommand("ORDER-FAIL-" + i, "CUST-" + i, 100.0);
            commandGateway.send(command, (cmdMsg, resultMsg) -> latch.countDown());
        }

        latch.await(2, TimeUnit.SECONDS);

        // Then
        Map<String, Object> metrics = commandGateway.getMetrics();
        assertThat(metrics.get("failedSends")).isEqualTo(3L);
    }

    @Test
    @DisplayName("Debe enviar comandos concurrentemente")
    void testConcurrentCommandSending() throws Exception {
        // Given
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(CompletableFuture.completedFuture(
                CommandReplyHandler.CommandResult.success("test", "OK")
            ));

        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        // When
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    CreateOrderCommand command = new CreateOrderCommand(
                        "ORDER-CONCURRENT-" + index,
                        "CUST-" + index,
                        100.0 + index
                    );
                    commandGateway.send(command, (cmdMsg, resultMsg) -> {});
                } finally {
                    latch.countDown();
                }
            });
        }

        // Then
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        
        Map<String, Object> metrics = commandGateway.getMetrics();
        assertThat(metrics.get("sentCommands")).isEqualTo((long) threadCount);
        
        executor.shutdown();
    }

    @Test
    @DisplayName("Debe extraer routing key correctamente del comando")
    void testRoutingKeyExtraction() throws Exception {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-ROUTING-001", "CUST-999", 200.0);
        
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenReturn(sendFuture);
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(new CompletableFuture<>());

        // When
        commandGateway.send(command, (cmdMsg, resultMsg) -> {});

        // Then
        verify(kafkaTemplate).send(
            eq("test-commands"),
            eq("ORDER-ROUTING-001"), // Routing key debe ser el orderId
            anyString()
        );
    }

    @Test
    @DisplayName("Debe incluir correlationId en metadata")
    void testCorrelationIdInMetadata() throws Exception {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-008", "CUST-444", 175.0);
        
        CompletableFuture<SendResult<String, String>> sendFuture = 
            CompletableFuture.completedFuture(mock(SendResult.class));
        
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .thenAnswer(invocation -> {
                String serializedCommand = invocation.getArgument(2);
                assertThat(serializedCommand).contains("correlationId");
                return sendFuture;
            });
        
        when(replyHandler.registerPendingCommand(anyString()))
            .thenReturn(new CompletableFuture<>());

        // When
        commandGateway.send(command, (cmdMsg, resultMsg) -> {});

        // Then
        verify(kafkaTemplate).send(anyString(), anyString(), anyString());
    }
}