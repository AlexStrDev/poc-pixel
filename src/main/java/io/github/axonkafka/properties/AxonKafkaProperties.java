package io.github.axonkafka.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties de configuración para Axon + Kafka Starter.
 * 
 * Ejemplo de uso en application.properties:
 * 
 * axon.kafka.bootstrap-servers=localhost:9092
 * axon.kafka.command.topic=my-app-commands
 * axon.kafka.event.topic=my-app-events
 * axon.kafka.topic.partitions=100
 * axon.kafka.topic.auto-correct-partitions=true
 */
@Data
@ConfigurationProperties(prefix = "axon.kafka")
public class AxonKafkaProperties {

    /**
     * Bootstrap servers de Kafka
     */
    private String bootstrapServers = "localhost:9092";

    /**
     * Configuración de comandos
     */
    private Command command = new Command();

    /**
     * Configuración de eventos
     */
    private Event event = new Event();

    /**
     * Configuración de materialización
     */
    private Materializer materializer = new Materializer();

    /**
     * Configuración de SAGA
     */
    private Saga saga = new Saga();

    /**
     * Configuración de topics
     */
    private Topic topic = new Topic();

    @Data
    public static class Command {
        /**
         * Nombre del topic de comandos
         */
        private String topic = "axon-commands";

        /**
         * Group ID para consumir comandos
         */
        private String groupId = "axon-command-handlers";

        /**
         * Concurrencia del consumer
         */
        private int concurrency = 3;

        /**
         * Timeout en segundos para respuestas
         */
        private long timeoutSeconds = 30;

        /**
         * Topic de respuestas (request-reply)
         */
        private String replyTopic = "axon-command-replies";

        /**
         * Group ID para respuestas
         */
        private String replyGroupId = "axon-command-reply-handlers";

        /**
         * Topic de Dead Letter Queue
         */
        private String dlqTopic = "axon-commands-dlq";
    }

    @Data
    public static class Event {
        /**
         * Nombre del topic de eventos
         */
        private String topic = "axon-events";

        /**
         * Group ID para procesadores de eventos
         */
        private String groupId = "axon-event-processors";

        /**
         * Concurrencia del consumer
         */
        private int concurrency = 3;
    }

    @Data
    public static class Materializer {
        /**
         * Group ID para materialización
         */
        private String groupId = "axon-event-materializer";

        /**
         * Concurrencia del materializador
         */
        private int concurrency = 5;

        /**
         * Timeout en segundos para materialización
         */
        private long timeoutSeconds = 30;

        /**
         * Tamaño del pool de consumers
         */
        private int consumerPoolSize = 5;
    }

    @Data
    public static class Saga {
        /**
         * Habilitar autoconfiguración de SAGA
         */
        private boolean enabled = true;

        /**
         * Nombre del processor de SAGA
         */
        private String processorName = "AxonSagaProcessor";
    }

    @Data
    public static class Topic {
        /**
         * Crear topics automáticamente si no existen
         */
        private boolean autoCreate = true;

        /**
         * Auto-corregir particiones si hay mismatch
         * Si está habilitado, incrementará automáticamente las particiones
         * de topics existentes que tengan menos particiones de las configuradas.
         * 
         * IMPORTANTE: Solo puede incrementar particiones, no reducirlas.
         */
        private boolean autoCorrectPartitions = true;

        /**
         * Número de particiones para topics
         */
        private int partitions = 3;

        /**
         * Factor de replicación
         */
        private short replicationFactor = 1;

        /**
         * Retención de mensajes en horas
         */
        private int retentionHours = 168; // 7 días
    }
}