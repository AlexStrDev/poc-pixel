package com.axonkafka.starter.storage;

import com.axonkafka.starter.bus.KafkaEventBus;
import com.axonkafka.starter.lock.DistributedLockService;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventsourcing.eventstore.jpa.JpaEventStorageEngine;
import org.axonframework.serialization.Serializer;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class KafkaEventStorageEngine extends JpaEventStorageEngine {

    private final KafkaEventBus kafkaEventBus;
    private final EventStoreMaterializer materializer;
    private final DistributedLockService lockService;

    protected KafkaEventStorageEngine(Builder builder) {
        super(builder);
        this.kafkaEventBus = builder.kafkaEventBus;
        this.materializer = builder.materializer;
        this.lockService = builder.lockService;
    }

    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        log.debug("📝 Persistiendo {} eventos - Kafka ÚNICAMENTE (source of truth)", events.size());

        events.stream()
                .filter(event -> event instanceof DomainEventMessage)
                .map(event -> (DomainEventMessage<?>) event)
                .forEach(event -> {
                    try {
                        kafkaEventBus.publish(event);
                        log.debug("✅ Evento publicado en Kafka: {} seq={}",
                                event.getAggregateIdentifier(), event.getSequenceNumber());
                    } catch (Exception e) {
                        log.error("💥 CRÍTICO: Error publicando evento a Kafka", e);
                        throw new RuntimeException("No se pudo persistir en Kafka (source of truth)", e);
                    }
                });

        log.info("✅ {} eventos publicados exitosamente a Kafka (source of truth)", events.size());
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readEventData(
            String aggregateIdentifier, long firstSequenceNumber) {

        log.debug("📖 Leyendo eventos: aggregate={}, desde seq={}",
                aggregateIdentifier, firstSequenceNumber);

        if (materializer.isMaterialized(aggregateIdentifier)) {
            log.debug("✅ Cache hit: Aggregate {} encontrado en PostgreSQL", aggregateIdentifier);
            return super.readEventData(aggregateIdentifier, firstSequenceNumber);
        }

        log.info("⚠️ Cache miss: Aggregate {} NO en PG - Materializando desde Kafka...",
                aggregateIdentifier);

        String lockKey = "materialize:" + aggregateIdentifier;

        try {
            boolean executed = lockService.executeWithLock(
                    lockKey,
                    30,
                    TimeUnit.SECONDS,
                    () -> {
                        if (!materializer.isMaterialized(aggregateIdentifier)) {
                            log.info("🔄 Materializando aggregate {} desde Kafka...", aggregateIdentifier);
                            materializer.materializeFromKafka(aggregateIdentifier);
                            log.info("✅ Aggregate {} materializado desde Kafka", aggregateIdentifier);
                        } else {
                            log.debug("✅ Aggregate {} ya fue materializado por otro thread",
                                    aggregateIdentifier);
                        }
                    }
            );

            if (!executed) {
                log.warn("⚠️ Timeout adquiriendo lock para materializar aggregate {}",
                        aggregateIdentifier);
                throw new RuntimeException("Timeout materializando aggregate desde Kafka");
            }

            return super.readEventData(aggregateIdentifier, firstSequenceNumber);

        } catch (Exception e) {
            log.error("💥 Error materializando aggregate desde Kafka", e);
            throw new RuntimeException("No se pudo reconstruir aggregate desde Kafka", e);
        }
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        log.debug("📸 Guardando snapshot: aggregate={}, seq={}",
                snapshot.getAggregateIdentifier(), snapshot.getSequenceNumber());
        super.storeSnapshot(snapshot, serializer);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends JpaEventStorageEngine.Builder {
        private KafkaEventBus kafkaEventBus;
        private EventStoreMaterializer materializer;
        private DistributedLockService lockService;

        @Override
        public Builder snapshotSerializer(Serializer snapshotSerializer) {
            super.snapshotSerializer(snapshotSerializer);
            return this;
        }

        @Override
        public Builder eventSerializer(Serializer eventSerializer) {
            super.eventSerializer(eventSerializer);
            return this;
        }

        @Override
        public Builder upcasterChain(org.axonframework.serialization.upcasting.event.EventUpcaster upcasterChain) {
            super.upcasterChain(upcasterChain);
            return this;
        }

        @Override
        public Builder persistenceExceptionResolver(
                org.axonframework.common.jdbc.PersistenceExceptionResolver persistenceExceptionResolver) {
            super.persistenceExceptionResolver(persistenceExceptionResolver);
            return this;
        }

        @Override
        public Builder entityManagerProvider(EntityManagerProvider entityManagerProvider) {
            super.entityManagerProvider(entityManagerProvider);
            return this;
        }

        @Override
        public Builder transactionManager(TransactionManager transactionManager) {
            super.transactionManager(transactionManager);
            return this;
        }

        @Override
        public Builder batchSize(int batchSize) {
            super.batchSize(batchSize);
            return this;
        }

        public Builder kafkaEventBus(KafkaEventBus kafkaEventBus) {
            this.kafkaEventBus = kafkaEventBus;
            return this;
        }

        public Builder materializer(EventStoreMaterializer materializer) {
            this.materializer = materializer;
            return this;
        }

        public Builder lockService(DistributedLockService lockService) {
            this.lockService = lockService;
            return this;
        }

        @Override
        public KafkaEventStorageEngine build() {
            if (kafkaEventBus == null) {
                throw new IllegalStateException("KafkaEventBus no puede ser null");
            }
            if (materializer == null) {
                throw new IllegalStateException("EventStoreMaterializer no puede ser null");
            }
            if (lockService == null) {
                throw new IllegalStateException("DistributedLockService no puede ser null");
            }
            return new KafkaEventStorageEngine(this);
        }
    }
}