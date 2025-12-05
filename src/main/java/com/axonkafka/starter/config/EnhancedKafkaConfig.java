package com.axonkafka.starter.config;

import com.axonkafka.starter.properties.AxonKafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
public class EnhancedKafkaConfig {

    private final AxonKafkaProperties properties;

    public EnhancedKafkaConfig(AxonKafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    @Qualifier("materializerConsumerFactory")
    public ConsumerFactory<String, String> materializerConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getEvent().getMaterializerGroupId());
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        log.info("✅ Configurando MaterializerConsumer con group-id: {}", properties.getEvent().getMaterializerGroupId());
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> materializerKafkaListenerContainerFactory(
            @Qualifier("materializerConsumerFactory") ConsumerFactory<String, String> materializerConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(materializerConsumerFactory);
        factory.setConcurrency(properties.getMaterializer().getConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setSyncCommits(true);
        factory.setBatchListener(false);

        log.info("✅ MaterializerKafkaListenerContainerFactory configurado con concurrencia: {}",
                properties.getMaterializer().getConcurrency());
        return factory;
    }

    @Bean
    @Qualifier("commandReplyConsumerFactory")
    public ConsumerFactory<String, String> commandReplyConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getCommand().getReplyGroupId());
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        log.info("✅ Configurando CommandReplyConsumer con group-id: {}", properties.getCommand().getReplyGroupId());
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> commandReplyKafkaListenerContainerFactory(
            @Qualifier("commandReplyConsumerFactory") ConsumerFactory<String, String> commandReplyConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(commandReplyConsumerFactory);
        factory.setConcurrency(2);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        log.info("✅ CommandReplyKafkaListenerContainerFactory configurado con concurrencia: 2");
        return factory;
    }
}