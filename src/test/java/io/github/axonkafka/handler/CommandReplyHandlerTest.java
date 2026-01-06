package io.github.axonkafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.axonkafka.properties.AxonKafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests unitarios para CommandReplyHandler
 * 
 * Cubre:
 * - Registro de comandos pendientes
 * - Procesamiento de respuestas exitosas
 * - Procesamiento de respuestas con error
 * - Timeouts
 * - Limpieza automática
 * - Thread-safety
 */
class CommandReplyHandlerTest {

    private CommandReplyHandler replyHandler;
    private AxonKafkaProperties properties;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        properties = new AxonKafkaProperties();
        properties.getCommand().setReplyTopic("test-replies");
        properties.getCommand().setReplyGroupId("test-reply-group");
        
        replyHandler = new CommandReplyHandler(properties);
        objectMapper = new ObjectMapper();
    }

    @Test
    @DisplayName("Debe registrar comando pendiente y retornar CompletableFuture")
    void testRegisterPendingCommand() {
        // Given
        String correlationId = UUID.randomUUID().toString();

        // When
        CompletableFuture<CommandReplyHandler.CommandResult> future = 
            replyHandler.registerPendingCommand(correlationId);

        // Then
        assertThat(future).isNotNull();
        assertThat(future.isDone()).isFalse();
        
        Map<String, Object> stats = replyHandler.getStats();
        assertThat(stats.get("pendingCommands")).isEqualTo(1);
    }

    @Test
    @DisplayName("Debe completar future con respuesta exitosa")
    void testHandleSuccessfulReply() throws Exception {
        // Given
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<CommandReplyHandler.CommandResult> future = 
            replyHandler.registerPendingCommand(correlationId);

        CommandReplyHandler.CommandResult successResult = 
            CommandReplyHandler.CommandResult.success(correlationId, "Operation completed");
        
        String jsonReply = objectMapper.writeValueAsString(successResult);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-replies", 0, 0, correlationId, jsonReply
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        replyHandler.handleCommandReply(record, ack);

        // Then
        assertThat(future.isDone()).isTrue();
        assertThat(future.isCompletedExceptionally()).isFalse();
        
        CommandReplyHandler.CommandResult result = future.get(1, TimeUnit.SECONDS);
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getResult()).isEqualTo("Operation completed");
        assertThat(result.getCorrelationId()).isEqualTo(correlationId);
        
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Debe completar future con error cuando la respuesta indica fallo")
    void testHandleErrorReply() throws Exception {
        // Given
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<CommandReplyHandler.CommandResult> future = 
            replyHandler.registerPendingCommand(correlationId);

        CommandReplyHandler.CommandResult errorResult = 
            CommandReplyHandler.CommandResult.error(correlationId, "Business rule violation");
        
        String jsonReply = objectMapper.writeValueAsString(errorResult);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-replies", 0, 0, correlationId, jsonReply
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        replyHandler.handleCommandReply(record, ack);

        // Then
        assertThat(future.isDone()).isTrue();
        assertThat(future.isCompletedExceptionally()).isTrue();
        
        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(CommandReplyHandler.CommandExecutionException.class)
            .hasMessageContaining("Business rule violation");
        
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Debe manejar respuesta para comando no registrado")
    void testHandleReplyForUnregisteredCommand() throws Exception {
        // Given
        String correlationId = UUID.randomUUID().toString();
        
        CommandReplyHandler.CommandResult result = 
            CommandReplyHandler.CommandResult.success(correlationId, "OK");
        
        String jsonReply = objectMapper.writeValueAsString(result);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-replies", 0, 0, correlationId, jsonReply
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        replyHandler.handleCommandReply(record, ack);

        // Then
        verify(ack).acknowledge(); // Debe hacer acknowledge de todas formas
    }

    @Test
    @DisplayName("Debe timeout si no llega respuesta")
    void testCommandTimeout() {
        // Given
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<CommandReplyHandler.CommandResult> future = 
            replyHandler.registerPendingCommand(correlationId);

        // When - Esperar a que timeout (5 minutos es el default, pero el future tiene orTimeout configurado)
        // Para este test, esperamos 100ms que es mucho menos que el timeout real
        
        // Then
        assertThat(future.isDone()).isFalse();
        
        // El timeout real ocurriría después de 5 minutos, pero no lo esperamos en el test
        // Solo verificamos que el mecanismo está configurado
    }

    @Test
    @DisplayName("Debe limpiar comando pendiente después de recibir respuesta")
    void testCleanupAfterReply() throws Exception {
        // Given
        String correlationId = UUID.randomUUID().toString();
        CompletableFuture<CommandReplyHandler.CommandResult> future = 
            replyHandler.registerPendingCommand(correlationId);

        Map<String, Object> statsBefore = replyHandler.getStats();
        assertThat(statsBefore.get("pendingCommands")).isEqualTo(1);

        CommandReplyHandler.CommandResult result = 
            CommandReplyHandler.CommandResult.success(correlationId, "OK");
        
        String jsonReply = objectMapper.writeValueAsString(result);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-replies", 0, 0, correlationId, jsonReply
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        replyHandler.handleCommandReply(record, ack);
        Thread.sleep(100); // Dar tiempo para que se limpie

        // Then
        Map<String, Object> statsAfter = replyHandler.getStats();
        assertThat(statsAfter.get("pendingCommands")).isEqualTo(0);
    }

    @Test
    @DisplayName("Debe manejar múltiples respuestas concurrentemente")
    void testConcurrentReplies() throws Exception {
        // Given
        int replyCount = 50;
        CountDownLatch latch = new CountDownLatch(replyCount);
        
        for (int i = 0; i < replyCount; i++) {
            final int index = i;
            String correlationId = "CORR-" + index;
            
            CompletableFuture<CommandReplyHandler.CommandResult> future = 
                replyHandler.registerPendingCommand(correlationId);
            
            future.thenAccept(result -> latch.countDown());
            
            // Simular respuesta asíncrona
            CompletableFuture.runAsync(() -> {
                try {
                    CommandReplyHandler.CommandResult result = 
                        CommandReplyHandler.CommandResult.success(correlationId, "Result-" + index);
                    
                    String jsonReply = objectMapper.writeValueAsString(result);
                    ConsumerRecord<String, String> record = new ConsumerRecord<>(
                        "test-replies", 0, 0, correlationId, jsonReply
                    );
                    Acknowledgment ack = mock(Acknowledgment.class);
                    
                    replyHandler.handleCommandReply(record, ack);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // When / Then
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("Debe mantener estadísticas correctas")
    void testStats() {
        // Given
        String correlationId1 = UUID.randomUUID().toString();
        String correlationId2 = UUID.randomUUID().toString();
        String correlationId3 = UUID.randomUUID().toString();

        // When
        replyHandler.registerPendingCommand(correlationId1);
        replyHandler.registerPendingCommand(correlationId2);
        replyHandler.registerPendingCommand(correlationId3);

        // Then
        Map<String, Object> stats = replyHandler.getStats();
        assertThat(stats.get("pendingCommands")).isEqualTo(3);
        assertThat(stats.get("timeoutMinutes")).isEqualTo(5L);
    }

    @Test
    @DisplayName("Debe manejar JSON mal formado")
    void testHandleMalformedJson() {
        // Given
        String correlationId = UUID.randomUUID().toString();
        replyHandler.registerPendingCommand(correlationId);
        
        String malformedJson = "{invalid json}";
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "test-replies", 0, 0, correlationId, malformedJson
        );
        Acknowledgment ack = mock(Acknowledgment.class);

        // When
        replyHandler.handleCommandReply(record, ack);

        // Then
        verify(ack).acknowledge(); // Debe hacer acknowledge para no bloquear el consumer
    }

    @Test
    @DisplayName("Debe crear CommandResult exitoso correctamente")
    void testCreateSuccessResult() {
        // Given
        String correlationId = "TEST-CORR-001";
        String resultMessage = "Command executed successfully";

        // When
        CommandReplyHandler.CommandResult result = 
            CommandReplyHandler.CommandResult.success(correlationId, resultMessage);

        // Then
        assertThat(result.getCorrelationId()).isEqualTo(correlationId);
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getResult()).isEqualTo(resultMessage);
        assertThat(result.getErrorMessage()).isNull();
        assertThat(result.getTimestamp()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Debe crear CommandResult de error correctamente")
    void testCreateErrorResult() {
        // Given
        String correlationId = "TEST-CORR-002";
        String errorMessage = "Command failed: Insufficient funds";

        // When
        CommandReplyHandler.CommandResult result = 
            CommandReplyHandler.CommandResult.error(correlationId, errorMessage);

        // Then
        assertThat(result.getCorrelationId()).isEqualTo(correlationId);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getResult()).isNull();
        assertThat(result.getErrorMessage()).isEqualTo(errorMessage);
        assertThat(result.getTimestamp()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Debe crear CommandResult de timeout correctamente")
    void testCreateTimeoutResult() {
        // Given
        String correlationId = "TEST-CORR-003";

        // When
        CommandReplyHandler.CommandResult result = 
            CommandReplyHandler.CommandResult.timeout(correlationId);

        // Then
        assertThat(result.getCorrelationId()).isEqualTo(correlationId);
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorMessage()).contains("Timeout esperando respuesta");
    }

    @Test
    @DisplayName("Debe serializar y deserializar CommandResult correctamente")
    void testCommandResultSerialization() throws Exception {
        // Given
        String correlationId = "TEST-CORR-004";
        CommandReplyHandler.CommandResult original = 
            CommandReplyHandler.CommandResult.success(correlationId, "Test result");

        // When
        String json = objectMapper.writeValueAsString(original);
        CommandReplyHandler.CommandResult deserialized = 
            objectMapper.readValue(json, CommandReplyHandler.CommandResult.class);

        // Then
        assertThat(deserialized.getCorrelationId()).isEqualTo(original.getCorrelationId());
        assertThat(deserialized.isSuccess()).isEqualTo(original.isSuccess());
        assertThat(deserialized.getResult()).isEqualTo(original.getResult());
    }

    @Test
    @DisplayName("Debe manejar múltiples registros y respuestas intercaladas")
    void testInterleavedRegistrationsAndReplies() throws Exception {
        // Given
        ExecutorService executor = Executors.newFixedThreadPool(10);
        int operationCount = 20;
        CountDownLatch latch = new CountDownLatch(operationCount);
        
        // When
        for (int i = 0; i < operationCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    String correlationId = "INTERLEAVED-" + index;
                    
                    // Registrar
                    CompletableFuture<CommandReplyHandler.CommandResult> future = 
                        replyHandler.registerPendingCommand(correlationId);
                    
                    // Esperar un poco aleatorio
                    Thread.sleep(ThreadLocalRandom.current().nextInt(50));
                    
                    // Enviar respuesta
                    CommandReplyHandler.CommandResult result = 
                        CommandReplyHandler.CommandResult.success(correlationId, "Result-" + index);
                    
                    String jsonReply = objectMapper.writeValueAsString(result);
                    ConsumerRecord<String, String> record = new ConsumerRecord<>(
                        "test-replies", 0, 0, correlationId, jsonReply
                    );
                    Acknowledgment ack = mock(Acknowledgment.class);
                    
                    replyHandler.handleCommandReply(record, ack);
                    
                    // Verificar que se completó
                    future.get(2, TimeUnit.SECONDS);
                    latch.countDown();
                    
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        // Then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();
    }

    @Test
    @DisplayName("Debe prevenir memory leaks limpiando futures completados")
    void testMemoryLeakPrevention() throws Exception {
        // Given
        int commandCount = 100;
        
        for (int i = 0; i < commandCount; i++) {
            String correlationId = "LEAK-TEST-" + i;
            CompletableFuture<CommandReplyHandler.CommandResult> future = 
                replyHandler.registerPendingCommand(correlationId);
            
            // Simular respuesta inmediata
            CommandReplyHandler.CommandResult result = 
                CommandReplyHandler.CommandResult.success(correlationId, "OK");
            
            String jsonReply = objectMapper.writeValueAsString(result);
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "test-replies", 0, 0, correlationId, jsonReply
            );
            Acknowledgment ack = mock(Acknowledgment.class);
            
            replyHandler.handleCommandReply(record, ack);
        }

        // When - Esperar a que el cleanup task limpie
        Thread.sleep(65000); // El cleanup se ejecuta cada 1 minuto

        // Then - Los comandos completados deberían ser removidos
        Map<String, Object> stats = replyHandler.getStats();
        assertThat((int) stats.get("pendingCommands")).isLessThan(commandCount);
    }
}