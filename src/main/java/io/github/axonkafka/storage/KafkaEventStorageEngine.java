package io.github.axonkafka.storage;

import io.github.axonkafka.bus.KafkaEventBus;
import io.github.axonkafka.lock.DistributedLockService;
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

/**
 * EventStorageEngine híbrido genérico:
 * 
 * ESCRITURA: Kafka únicamente (source of truth)
 * LECTURA: PostgreSQL (cache) con lazy-load desde Kafka
 * 
 * MEJORA: Detecta cuando PG está vacío y fuerza re-materialización
 */
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
        log.debug("📝 Persistiendo {} eventos - Kafka ÚNICAMENTE", events.size());
        
        events.stream()
            .filter(event -> event instanceof DomainEventMessage)
            .map(event -> (DomainEventMessage<?>) event)
            .forEach(event -> {
                try {
                    kafkaEventBus.publish(event);
                    log.debug("✅ Evento publicado: {} seq={}", 
                        event.getAggregateIdentifier(), event.getSequenceNumber());
                } catch (Exception e) {
                    log.error("💥 CRÍTICO: Error publicando evento a Kafka", e);
                    throw new RuntimeException("No se pudo persistir en Kafka", e);
                }
            });
        
        log.info("✅ {} eventos publicados a Kafka", events.size());
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readEventData(
            String aggregateIdentifier, long firstSequenceNumber) {
        
        log.debug("📖 Leyendo eventos: aggregate={}, desde seq={}", 
            aggregateIdentifier, firstSequenceNumber);
        
        boolean markedAsMaterialized = materializer.isMaterialized(aggregateIdentifier);
        
        if (markedAsMaterialized) {
            log.debug("✅ Cache indica materializado, verificando PG...");
            
            Stream<? extends DomainEventData<?>> pgStream = 
                super.readEventData(aggregateIdentifier, firstSequenceNumber);
            
            List<? extends DomainEventData<?>> events = pgStream.toList();
            
            if (!events.isEmpty()) {
                log.debug("✅ Datos encontrados en PG: {} eventos", events.size());
                return events.stream();
            }
            
            log.warn("⚠️ PG vacío pero cache indica materializado - Forzando re-materialización");
            materializer.removeMaterializedMark(aggregateIdentifier);
        }
        
        log.info("🔄 Materializando desde Kafka...");
        
        String lockKey = "materialize:" + aggregateIdentifier;
        
        try {
            boolean executed = lockService.executeWithLock(
                lockKey, 
                30, 
                TimeUnit.SECONDS,
                () -> {
                    if (!hasEventsInPostgreSQL(aggregateIdentifier)) {
                        log.info("🔄 Materializando aggregate: {}", aggregateIdentifier);
                        materializer.materializeFromKafka(aggregateIdentifier);
                        log.info("✅ Aggregate materializado: {}", aggregateIdentifier);
                    } else {
                        log.debug("✅ Ya materializado por otro thread");
                    }
                }
            );
            
            if (!executed) {
                log.warn("⚠️ Timeout adquiriendo lock: {}", aggregateIdentifier);
                throw new RuntimeException("Timeout materializando aggregate");
            }
            return super.readEventData(aggregateIdentifier, firstSequenceNumber);
        } catch (Exception e) {
            log.error("💥 Error materializando aggregate", e);
            throw new RuntimeException("No se pudo reconstruir aggregate", e);
        }
    }

    private boolean hasEventsInPostgreSQL(String aggregateIdentifier) {
        try {
            Stream<? extends DomainEventData<?>> stream = 
                super.readEventData(aggregateIdentifier, 0);
            return stream.findFirst().isPresent();
            
        } catch (Exception e) {
            log.warn("Error verificando eventos en PG: {}", e.getMessage());
            return false;
        }
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        log.debug("📸 Guardando snapshot: aggregate={}", snapshot.getAggregateIdentifier());
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
                throw new IllegalStateException("KafkaEventBus es requerido");
            }
            if (materializer == null) {
                throw new IllegalStateException("EventStoreMaterializer es requerido");
            }
            if (lockService == null) {
                throw new IllegalStateException("DistributedLockService es requerido");
            }
            return new KafkaEventStorageEngine(this);
        }
    }
}