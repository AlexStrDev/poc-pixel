package com.axonkafka.starter;

import com.axonkafka.starter.bus.KafkaEventBus;
import com.axonkafka.starter.gateway.KafkaCommandGateway;
import com.axonkafka.starter.lock.DistributedLockService;
import com.axonkafka.starter.properties.AxonKafkaProperties;
import com.axonkafka.starter.storage.EventStoreMaterializer;
import com.axonkafka.starter.storage.KafkaEventStorageEngine;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.interceptors.BeanValidationInterceptor;
import org.axonframework.messaging.interceptors.LoggingInterceptor;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Slf4j
@AutoConfiguration
@EnableConfigurationProperties(AxonKafkaProperties.class)
@ConditionalOnProperty(prefix = "axon.kafka", name = "enabled", havingValue = "true", matchIfMissing = true)
@Import({
    com.axonkafka.starter.config.KafkaCommandConfig.class,
    com.axonkafka.starter.config.KafkaEventConfig.class,
    com.axonkafka.starter.config.EnhancedKafkaConfig.class,
    com.axonkafka.starter.config.KafkaTopicConfig.class,
    com.axonkafka.starter.serializer.CommandSerializer.class,
    com.axonkafka.starter.serializer.KafkaEventSerializer.class,
    com.axonkafka.starter.handler.CommandReplyHandler.class,
    com.axonkafka.starter.bus.KafkaCommandBus.class,
    com.axonkafka.starter.bus.KafkaEventBus.class,
    com.axonkafka.starter.gateway.KafkaCommandGateway.class,
    com.axonkafka.starter.storage.EventStoreMaterializer.class,
    com.axonkafka.starter.consumer.EventMaterializationConsumer.class,
    com.axonkafka.starter.cache.CommandDeduplicationService.class,
    com.axonkafka.starter.lock.DistributedLockService.class
})
public class AxonKafkaAutoConfiguration {

    @Bean
    @Primary
    public CommandBus localCommandBus() {
        SimpleCommandBus commandBus = SimpleCommandBus.builder().build();
        
        commandBus.registerDispatchInterceptor(new BeanValidationInterceptor<>());
        commandBus.registerDispatchInterceptor(new LoggingInterceptor<>());
        commandBus.registerHandlerInterceptor(new LoggingInterceptor<>());
        
        log.info("✅ CommandBus local configurado con interceptors");
        return commandBus;
    }

    @Bean
    @Primary
    public CommandGateway commandGateway(KafkaCommandGateway kafkaCommandGateway) {
        log.info("✅ Usando KafkaCommandGateway como CommandGateway principal");
        return kafkaCommandGateway;
    }

    @Bean
    public KafkaEventStorageEngine eventStorageEngine(
            Serializer defaultSerializer,
            EntityManagerProvider entityManagerProvider,
            TransactionManager transactionManager,
            KafkaEventBus kafkaEventBus,
            EventStoreMaterializer materializer,
            DistributedLockService lockService) {
        
        log.info("🔧 Configurando KafkaEventStorageEngine híbrido:");
        log.info("   📝 Escritura: Kafka ÚNICAMENTE (source of truth)");
        log.info("   📖 Lectura: PostgreSQL con lazy-load desde Kafka");
        log.info("   🔒 Lock distribuido: Previene materialización concurrente");
        
        return KafkaEventStorageEngine.builder()
                .snapshotSerializer(defaultSerializer)
                .eventSerializer(defaultSerializer)
                .entityManagerProvider(entityManagerProvider)
                .transactionManager(transactionManager)
                .kafkaEventBus(kafkaEventBus)
                .materializer(materializer)
                .lockService(lockService)
                .build();
    }

    @Autowired
    public void configureEventProcessing(EventProcessingConfigurer configurer) {
        configurer.registerDefaultListenerInvocationErrorHandler(
                configuration -> (exception, event, eventHandler) -> {
                    log.error("❌ Error procesando evento: {}", event, exception);
                }
        );
        
        log.info("✅ Configuración de procesamiento de eventos completada");
    }

    @Bean
    public EventStore eventStore(KafkaEventStorageEngine eventStorageEngine) {
        return org.axonframework.eventsourcing.eventstore.EmbeddedEventStore.builder()
                .storageEngine(eventStorageEngine)
                .build();
    }
}