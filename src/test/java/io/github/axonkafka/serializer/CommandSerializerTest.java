package io.github.axonkafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.modelling.command.TargetAggregateIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests unitarios para CommandSerializer
 * 
 * Cubre:
 * - Serialización de comandos simples
 * - Serialización con metadata
 * - Deserialización correcta
 * - Extracción de routing keys
 * - Manejo de errores
 */
class CommandSerializerTest {

    private CommandSerializer serializer;
    private ObjectMapper objectMapper;

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

        // Getters y setters
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }
        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }
    }

    // Comando sin @TargetAggregateIdentifier
    static class SimpleCommand {
        private String data;

        public SimpleCommand() {}
        public SimpleCommand(String data) { this.data = data; }

        public String getData() { return data; }
        public void setData(String data) { this.data = data; }
    }

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        serializer = new CommandSerializer(objectMapper);
    }

    @Test
    @DisplayName("Debe serializar comando simple correctamente")
    void testSerializeSimpleCommand() {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-001", "CUST-123", 150.50);
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command);
        String routingKey = "ORDER-001";

        // When
        String json = serializer.serialize(commandMessage, routingKey);

        // Then
        assertThat(json).isNotNull();
        assertThat(json).contains("CreateOrderCommand");
        assertThat(json).contains("ORDER-001");
        assertThat(json).contains("CUST-123");
        assertThat(json).contains("150.5");
        assertThat(json).contains("\"routingKey\":\"ORDER-001\"");
    }

    @Test
    @DisplayName("Debe incluir metadata en la serialización")
    void testSerializeCommandWithMetadata() {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-002", "CUST-456", 200.75);
        Map<String, Object> metadata = Map.of(
            "correlationId", UUID.randomUUID().toString(),
            "userId", "USER-789",
            "timestamp", System.currentTimeMillis()
        );
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(metadata);
        String routingKey = "ORDER-002";

        // When
        String json = serializer.serialize(commandMessage, routingKey);

        // Then
        assertThat(json).contains("correlationId");
        assertThat(json).contains("userId");
        assertThat(json).contains("USER-789");
    }

    @Test
    @DisplayName("Debe deserializar comando correctamente")
    void testDeserializeCommand() throws Exception {
        // Given
        CreateOrderCommand originalCommand = new CreateOrderCommand("ORDER-003", "CUST-999", 99.99);
        CommandMessage<?> originalMessage = GenericCommandMessage.asCommandMessage(originalCommand);
        String json = serializer.serialize(originalMessage, "ORDER-003");

        // When
        CommandMessage<?> deserializedMessage = serializer.deserialize(json);

        // Then
        assertThat(deserializedMessage).isNotNull();
        assertThat(deserializedMessage.getPayload()).isInstanceOf(CreateOrderCommand.class);
        
        CreateOrderCommand deserializedCommand = (CreateOrderCommand) deserializedMessage.getPayload();
        assertThat(deserializedCommand.getOrderId()).isEqualTo("ORDER-003");
        assertThat(deserializedCommand.getCustomerId()).isEqualTo("CUST-999");
        assertThat(deserializedCommand.getAmount()).isEqualTo(99.99);
    }

    @Test
    @DisplayName("Debe preservar metadata en ciclo serialize-deserialize")
    void testMetadataPreservation() {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-004", "CUST-111", 123.45);
        String correlationId = UUID.randomUUID().toString();
        CommandMessage<?> originalMessage = GenericCommandMessage.asCommandMessage(command)
            .andMetaData(Map.of("correlationId", correlationId, "source", "API"));

        String json = serializer.serialize(originalMessage, "ORDER-004");

        // When
        CommandMessage<?> deserializedMessage = serializer.deserialize(json);

        // Then
        assertThat(deserializedMessage.getMetaData().get("correlationId")).isEqualTo(correlationId);
        assertThat(deserializedMessage.getMetaData().get("source")).isEqualTo("API");
    }

    @Test
    @DisplayName("Debe extraer routing key de @TargetAggregateIdentifier")
    void testExtractRoutingKeyFromAnnotation() {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-005", "CUST-222", 250.00);
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command);

        // When
        String routingKey = serializer.extractRoutingKey(commandMessage);

        // Then
        assertThat(routingKey).isEqualTo("ORDER-005");
    }

    @Test
    @DisplayName("Debe generar UUID cuando no hay @TargetAggregateIdentifier")
    void testExtractRoutingKeyWithoutAnnotation() {
        // Given
        SimpleCommand command = new SimpleCommand("test-data");
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command);

        // When
        String routingKey = serializer.extractRoutingKey(commandMessage);

        // Then
        assertThat(routingKey).isNotNull();
        assertThat(routingKey).matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}");
    }

    @Test
    @DisplayName("Debe manejar comandos con campos null")
    void testSerializeCommandWithNullFields() {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-006", null, 100.00);
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command);

        // When
        String json = serializer.serialize(commandMessage, "ORDER-006");

        // Then
        assertThat(json).isNotNull();
        assertThat(json).contains("ORDER-006");
        
        // Deserializar y verificar
        CommandMessage<?> deserializedMessage = serializer.deserialize(json);
        CreateOrderCommand deserializedCommand = (CreateOrderCommand) deserializedMessage.getPayload();
        assertThat(deserializedCommand.getCustomerId()).isNull();
    }

    @Test
    @DisplayName("Debe lanzar excepción si JSON es inválido")
    void testDeserializeInvalidJson() {
        // Given
        String invalidJson = "{invalid json}";

        // When / Then
        assertThatThrownBy(() -> serializer.deserialize(invalidJson))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Error deserializando comando");
    }

    @Test
    @DisplayName("Debe lanzar excepción si clase no existe")
    void testDeserializeNonExistentClass() {
        // Given
        String jsonWithInvalidClass = """
            {
                "messageIdentifier": "test-id",
                "commandName": "com.nonexistent.FakeCommand",
                "timestamp": 1234567890,
                "payload": {
                    "type": "com.nonexistent.FakeCommand",
                    "revision": "1.0",
                    "data": "{\\"field\\":\\"value\\"}"
                },
                "metaData": {},
                "routingKey": "test-key"
            }
            """;

        // When / Then
        assertThatThrownBy(() -> serializer.deserialize(jsonWithInvalidClass))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Error deserializando comando");
    }

    @Test
    @DisplayName("Debe generar messageIdentifier único para cada comando")
    void testUniqueMessageIdentifiers() {
        // Given
        CreateOrderCommand command1 = new CreateOrderCommand("ORDER-007", "CUST-333", 50.00);
        CreateOrderCommand command2 = new CreateOrderCommand("ORDER-008", "CUST-444", 60.00);
        
        CommandMessage<?> message1 = GenericCommandMessage.asCommandMessage(command1);
        CommandMessage<?> message2 = GenericCommandMessage.asCommandMessage(command2);

        // When
        String json1 = serializer.serialize(message1, "ORDER-007");
        String json2 = serializer.serialize(message2, "ORDER-008");

        CommandMessage<?> deserialized1 = serializer.deserialize(json1);
        CommandMessage<?> deserialized2 = serializer.deserialize(json2);

        // Then
        assertThat(deserialized1.getIdentifier()).isNotEqualTo(deserialized2.getIdentifier());
    }

    @Test
    @DisplayName("Debe manejar comandos con números grandes")
    void testSerializeCommandWithLargeNumbers() {
        // Given
        CreateOrderCommand command = new CreateOrderCommand("ORDER-009", "CUST-555", Double.MAX_VALUE);
        CommandMessage<?> commandMessage = GenericCommandMessage.asCommandMessage(command);

        // When
        String json = serializer.serialize(commandMessage, "ORDER-009");
        CommandMessage<?> deserializedMessage = serializer.deserialize(json);

        // Then
        CreateOrderCommand deserializedCommand = (CreateOrderCommand) deserializedMessage.getPayload();
        assertThat(deserializedCommand.getAmount()).isEqualTo(Double.MAX_VALUE);
    }

    @Test
    @DisplayName("Debe serializar y deserializar correctamente comandos múltiples veces (idempotencia)")
    void testMultipleSerializationCycles() {
        // Given
        CreateOrderCommand originalCommand = new CreateOrderCommand("ORDER-010", "CUST-666", 175.25);
        CommandMessage<?> originalMessage = GenericCommandMessage.asCommandMessage(originalCommand);

        // When - Ciclo 1
        String json1 = serializer.serialize(originalMessage, "ORDER-010");
        CommandMessage<?> deserialized1 = serializer.deserialize(json1);
        
        // Ciclo 2
        String json2 = serializer.serialize(deserialized1, "ORDER-010");
        CommandMessage<?> deserialized2 = serializer.deserialize(json2);

        // Then
        CreateOrderCommand command1 = (CreateOrderCommand) deserialized1.getPayload();
        CreateOrderCommand command2 = (CreateOrderCommand) deserialized2.getPayload();
        
        assertThat(command1.getOrderId()).isEqualTo(command2.getOrderId());
        assertThat(command1.getCustomerId()).isEqualTo(command2.getCustomerId());
        assertThat(command1.getAmount()).isEqualTo(command2.getAmount());
    }
}