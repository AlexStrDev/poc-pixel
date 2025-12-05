package com.axonkafka.starter.config;

import com.axonkafka.starter.properties.AxonKafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración para crear tópicos de Kafka automáticamente al iniciar la aplicación.
 * 
 * Los tópicos se crean con:
 * - 1 partición (ajustable según necesidad)
 * - Factor de replicación 1 (ajustable en producción)
 * - Retención de 7 días
 */
@Slf4j
@Configuration
public class KafkaTopicConfig {

    private final AxonKafkaProperties properties;

    public KafkaTopicConfig(AxonKafkaProperties properties) {
        this.properties = properties;
    }

    /**
     * KafkaAdmin para gestionar tópicos
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, 
            properties.getKafka().getBootstrapServers());
        
        log.info("✅ KafkaAdmin configurado con bootstrap servers: {}", 
            properties.getKafka().getBootstrapServers());
        
        return new KafkaAdmin(configs);
    }

    /**
     * Tópico de comandos
     */
    @Bean
    public NewTopic commandTopic() {
        String topicName = properties.getCommand().getTopic();
        
        log.info("📝 Creando tópico de comandos: {}", topicName);
        
        return TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)
                .config("retention.ms", "604800000") // 7 días
                .build();
    }

    /**
     * Tópico de comandos DLQ (Dead Letter Queue)
     */
    @Bean
    public NewTopic commandDlqTopic() {
        String topicName = properties.getCommand().getDlqTopic();
        
        log.info("📮 Creando tópico de comandos DLQ: {}", topicName);
        
        return TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)
                .config("retention.ms", "2592000000") // 30 días (para análisis posterior)
                .build();
    }

    /**
     * Tópico de respuestas de comandos (Request-Reply pattern)
     */
    @Bean
    public NewTopic commandReplyTopic() {
        String topicName = properties.getCommand().getReplyTopic();
        
        log.info("📤 Creando tópico de respuestas de comandos: {}", topicName);
        
        return TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)
                .config("retention.ms", "3600000") // 1 hora (respuestas son efímeras)
                .build();
    }

    /**
     * Tópico de eventos (EventStore - source of truth)
     */
    @Bean
    public NewTopic eventTopic() {
        String topicName = properties.getEvent().getTopic();
        
        log.info("📊 Creando tópico de eventos (EventStore): {}", topicName);
        
        return TopicBuilder.name(topicName)
                .partitions(3) // Múltiples particiones para paralelismo
                .replicas(1)
                .config("retention.ms", "-1") // Retención infinita (source of truth)
                .config("cleanup.policy", "delete") // Por ahora delete, en producción usar "compact"
                .build();
    }
}