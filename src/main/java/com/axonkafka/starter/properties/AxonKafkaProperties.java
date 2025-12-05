package com.axonkafka.starter.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "axon.kafka")
public class AxonKafkaProperties {

    /**
     * Habilitar o deshabilitar el starter completo
     */
    private boolean enabled = true;

    /**
     * Configuración de Kafka
     */
    private Kafka kafka = new Kafka();

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
     * Configuración de cache/idempotencia
     */
    private Cache cache = new Cache();

    @Data
    public static class Kafka {
        private String bootstrapServers = "localhost:9092";
    }

    @Data
    public static class Command {
        private String topic = "axon-commands";
        private String groupId = "axon-command-handlers";
        private String dlqTopic = "axon-commands-dlq";
        private String replyTopic = "axon-command-replies";
        private String replyGroupId = "axon-command-reply-handlers";
        private int consumerConcurrency = 3;
        private long timeoutSeconds = 30;
    }

    @Data
    public static class Event {
        private String topic = "axon-events";
        private String groupId = "axon-event-processors";
        private String materializerGroupId = "axon-event-materializer";
        private int consumerConcurrency = 3;
    }

    @Data
    public static class Materializer {
        private boolean enabled = true;
        private int concurrency = 5;
        private long timeoutSeconds = 30;
        private int consumerPoolSize = 5;
        private int batchSize = 50;
    }

    @Data
    public static class Cache {
        private long retentionMinutes = 60;
    }
}