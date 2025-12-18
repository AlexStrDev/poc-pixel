package io.github.axonkafka.autoconfigure;

import io.github.axonkafka.bus.KafkaCommandBus;
import io.github.axonkafka.bus.KafkaEventBus;
import io.github.axonkafka.cache.CommandDeduplicationService;
import io.github.axonkafka.gateway.KafkaCommandGateway;
import io.github.axonkafka.handler.CommandReplyHandler;
import io.github.axonkafka.lock.DistributedLockService;
import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.CommandSerializer;
import io.github.axonkafka.serializer.KafkaEventSerializer;
import io.github.axonkafka.storage.EventMaterializationConsumer;
import io.github.axonkafka.storage.EventStoreMaterializer;
import io.github.axonkafka.storage.KafkaEventStorageEngine;
import io.github.axonkafka.topic.KafkaTopicCreator;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.jpa.JpaTokenStore;
import org.axonframework.messaging.interceptors.BeanValidationInterceptor;
import org.axonframework.messaging.interceptors.LoggingInterceptor;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.jpa.JpaSagaStore;
import org.axonframework.serialization.Serializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Autoconfiguración principal de Axon + Kafka Starter.
 * 
 * Proporciona:
 * - CommandBus con Kafka
 * - CommandGateway con request-reply
 * - EventStore híbrido (Kafka + PostgreSQL)
 * - Materialización asíncrona
 * - Idempotencia y locks distribuidos
 * - SAGA support
 */
@Slf4j
@AutoConfiguration
@EnableConfigurationProperties(AxonKafkaProperties.class)
@ConditionalOnProperty(prefix = "axon.kafka", name = "bootstrap-servers")
public class AxonKafkaAutoConfiguration {

    public AxonKafkaAutoConfiguration() {
        log.info("🚀 Inicializando Axon Kafka Spring Boot Starter");
    }

    // ========== Serializadores ==========
    
    @Bean
    @ConditionalOnMissingBean
    public CommandSerializer commandSerializer() {
        log.info("✅ CommandSerializer configurado");
        return new CommandSerializer();
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaEventSerializer kafkaEventSerializer() {
        log.info("✅ KafkaEventSerializer configurado");
        return new KafkaEventSerializer();
    }

    // ========== Cache y Locks ==========
    
    @Bean
    @ConditionalOnMissingBean
    public CommandDeduplicationService commandDeduplicationService() {
        log.info("✅ CommandDeduplicationService configurado");
        return new CommandDeduplicationService();
    }

    @Bean
    @ConditionalOnMissingBean
    public DistributedLockService distributedLockService() {
        log.info("✅ DistributedLockService configurado");
        return new DistributedLockService();
    }

    // ========== Handlers ==========
    
    @Bean
    @ConditionalOnMissingBean
    public CommandReplyHandler commandReplyHandler(AxonKafkaProperties properties) {
        log.info("✅ CommandReplyHandler configurado");
        return new CommandReplyHandler(properties);
    }

    // ========== Buses ==========
    
    @Bean
    @ConditionalOnMissingBean(name = "localCommandBus")
    public CommandBus localCommandBus() {
        SimpleCommandBus commandBus = SimpleCommandBus.builder().build();
        commandBus.registerDispatchInterceptor(new BeanValidationInterceptor<>());
        commandBus.registerDispatchInterceptor(new LoggingInterceptor<>());
        commandBus.registerHandlerInterceptor(new LoggingInterceptor<>());
        
        log.info("✅ Local CommandBus configurado");
        return commandBus;
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaCommandBus kafkaCommandBus(
            CommandBus localCommandBus,
            CommandSerializer commandSerializer,
            CommandDeduplicationService deduplicationService,
            KafkaTemplate<String, String> kafkaTemplate,
            AxonKafkaProperties properties) {
        
        log.info("✅ KafkaCommandBus configurado");
        return new KafkaCommandBus(
            localCommandBus,
            commandSerializer,
            deduplicationService,
            kafkaTemplate,
            properties
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaEventBus kafkaEventBus(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaEventSerializer eventSerializer,
            AxonKafkaProperties properties) {
        
        log.info("✅ KafkaEventBus configurado");
        return new KafkaEventBus(kafkaTemplate, eventSerializer, properties);
    }

    // ========== Gateway ==========
    
    @Bean
    @Primary
    @ConditionalOnMissingBean
    public CommandGateway commandGateway(
            KafkaTemplate<String, String> kafkaTemplate,
            CommandSerializer commandSerializer,
            CommandReplyHandler replyHandler,
            AxonKafkaProperties properties) {
        
        log.info("✅ KafkaCommandGateway configurado (Primary)");
        return new KafkaCommandGateway(
            kafkaTemplate,
            commandSerializer,
            replyHandler,
            properties
        );
    }

    // ========== EventStore ==========
    
    @Bean
    @ConditionalOnMissingBean
    public EventStoreMaterializer eventStoreMaterializer(
            EntityManagerProvider entityManagerProvider,
            KafkaEventSerializer eventSerializer,
            Serializer defaultSerializer,
            AxonKafkaProperties properties) {
        
        log.info("✅ EventStoreMaterializer configurado");
        return new EventStoreMaterializer(
            entityManagerProvider,
            eventSerializer,
            defaultSerializer,
            properties
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public EventMaterializationConsumer eventMaterializationConsumer(
            EntityManagerProvider entityManagerProvider,
            KafkaEventSerializer eventSerializer,
            Serializer defaultSerializer) {
        
        log.info("✅ EventMaterializationConsumer configurado");
        return new EventMaterializationConsumer(
            entityManagerProvider,
            eventSerializer,
            defaultSerializer
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaEventStorageEngine eventStorageEngine(
            Serializer defaultSerializer,
            EntityManagerProvider entityManagerProvider,
            TransactionManager transactionManager,
            KafkaEventBus kafkaEventBus,
            EventStoreMaterializer materializer,
            DistributedLockService lockService) {
        
        log.info("✅ KafkaEventStorageEngine (híbrido) configurado");
        log.info("   📝 Escritura: Kafka (source of truth)");
        log.info("   📖 Lectura: PostgreSQL con lazy-load");
        
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

    // ========== SAGA Support ==========
    
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "axon.kafka.saga", name = "enabled", havingValue = "true", matchIfMissing = true)
    public SagaStore<Object> sagaStore(
            Serializer serializer,
            EntityManagerProvider entityManagerProvider) {
        
        log.info("✅ JpaSagaStore configurado para Axon SAGAs");
        return JpaSagaStore.builder()
                .serializer(serializer)
                .entityManagerProvider(entityManagerProvider)
                .build();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "axon.kafka.saga", name = "enabled", havingValue = "true", matchIfMissing = true)
    public TokenStore tokenStore(
            Serializer serializer,
            EntityManagerProvider entityManagerProvider) {
        
        log.info("✅ JpaTokenStore configurado");
        return JpaTokenStore.builder()
                .serializer(serializer)
                .entityManagerProvider(entityManagerProvider)
                .build();
    }

    @Autowired
    @ConditionalOnProperty(prefix = "axon.kafka.saga", name = "enabled", havingValue = "true", matchIfMissing = true)
    public void configureEventProcessing(
            EventProcessingConfigurer configurer,
            AxonKafkaProperties properties) {
        
        log.info("🔧 Configurando EventProcessor para SAGAs");
        
        String processorName = properties.getSaga().getProcessorName();
        configurer.registerSubscribingEventProcessor(processorName);
        
        log.info("✅ {} configurado como SUBSCRIBING (event-driven)", processorName);
    }

    // ========== Topic Creator ==========
    
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "axon.kafka.topic", name = "auto-create", havingValue = "true", matchIfMissing = true)
    public KafkaTopicCreator kafkaTopicCreator(AxonKafkaProperties properties) {
        log.info("✅ KafkaTopicCreator configurado");
        return new KafkaTopicCreator(properties);
    }
}