package io.github.axonkafka.storage;

import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.KafkaEventSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.jpa.DomainEventEntry;
import org.axonframework.serialization.Serializer;

import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Materializa eventos desde Kafka a PostgreSQL de forma optimizada.
 * 
 * MEJORAS:
 * - Detección temprana de fin de topic (no esperar timeout completo)
 * - Timeout adaptativo basado en contexto
 * - Mejor manejo de particiones vacías
 * - Métricas de rendimiento
 */
@Slf4j
public class EventStoreMaterializer {

    private final EntityManagerProvider entityManagerProvider;
    private final KafkaEventSerializer eventSerializer;
    private final Serializer axonSerializer;
    private final AxonKafkaProperties properties;
    
    private final Set<String> materializedCache;
    private final Queue<KafkaConsumer<String, String>> consumerPool;
    
    // Métricas de rendimiento
    private long totalMaterializationTimeMs = 0;
    private int materializationCount = 0;

    public EventStoreMaterializer(
            EntityManagerProvider entityManagerProvider,
            KafkaEventSerializer eventSerializer,
            Serializer axonSerializer,
            AxonKafkaProperties properties) {
        
        this.entityManagerProvider = entityManagerProvider;
        this.eventSerializer = eventSerializer;
        this.axonSerializer = axonSerializer;
        this.properties = properties;
        this.materializedCache = ConcurrentHashMap.newKeySet();
        this.consumerPool = new LinkedList<>();
    }

    public boolean isMaterialized(String aggregateIdentifier) {
        if (materializedCache.contains(aggregateIdentifier)) {
            log.debug("✅ Cache hit: {}", aggregateIdentifier);
            return true;
        }
        
        EntityManager em = entityManagerProvider.getEntityManager();
        
        try {
            Long count = em.createQuery(
                "SELECT COUNT(e) FROM DomainEventEntry e WHERE e.aggregateIdentifier = :aggId",
                Long.class)
                .setParameter("aggId", aggregateIdentifier)
                .setMaxResults(1)
                .getSingleResult();
            
            boolean exists = count > 0;
            
            if (exists) {
                materializedCache.add(aggregateIdentifier);
                log.debug("✅ Encontrado en PG: {}", aggregateIdentifier);
            }
            
            return exists;
            
        } catch (Exception e) {
            log.warn("Error verificando materialización: {}", e.getMessage());
            return false;
        }
    }

    public void markAsMaterialized(String aggregateIdentifier) {
        materializedCache.add(aggregateIdentifier);
        log.debug("✅ Marcado como materializado: {}", aggregateIdentifier);
    }

    /**
     * Materializa eventos desde Kafka con detección temprana de fin de topic.
     */
    public void materializeFromKafka(String aggregateIdentifier) {
        long overallStartTime = System.currentTimeMillis();
        log.info("🔄 Materializando {} desde Kafka...", aggregateIdentifier);
        
        EntityManager em = entityManagerProvider.getEntityManager();
        List<DomainEventMessage<?>> events = new ArrayList<>();
        KafkaConsumer<String, String> consumer = null;
        
        try {
            consumer = getConsumerFromPool();
            
            // Configurar particiones
            List<TopicPartition> partitions = new ArrayList<>();
            String eventTopic = properties.getEvent().getTopic();
            consumer.partitionsFor(eventTopic).forEach(info -> 
                partitions.add(new TopicPartition(eventTopic, info.partition()))
            );
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            
            // Obtener offsets finales de cada partición
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            log.debug("📊 End offsets: {}", endOffsets);
            
            boolean found = false;
            long startTime = System.currentTimeMillis();
            long timeoutMillis = calculateTimeout(found);
            long lastEventSeq = -1;
            int emptyPollsInARow = 0;
            int maxEmptyPolls = 3; // Salir después de 3 polls vacíos consecutivos
            
            while (System.currentTimeMillis() - startTime < timeoutMillis) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                
                if (records.isEmpty()) {
                    emptyPollsInARow++;
                    
                    // MEJORA 1: Detección temprana de fin de topic
                    if (hasReachedEnd(consumer, partitions, endOffsets)) {
                        log.debug("🏁 Fin de topic alcanzado, saliendo...");
                        break;
                    }
                    
                    // MEJORA 2: Salir si ya encontramos eventos y no hay más datos
                    if (found) {
                        log.debug("✅ Eventos encontrados y no hay más datos");
                        break;
                    }
                    
                    // MEJORA 3: Salir temprano si múltiples polls vacíos y no se encontró nada
                    if (emptyPollsInARow >= maxEmptyPolls && !found) {
                        log.debug("⏭️ {} polls vacíos consecutivos sin encontrar eventos, saliendo...", 
                            emptyPollsInARow);
                        break;
                    }
                    
                    continue;
                }
                
                emptyPollsInARow = 0; // Reset contador
                
                for (ConsumerRecord<String, String> record : records) {
                    if (aggregateIdentifier.equals(record.key())) {
                        try {
                            DomainEventMessage<?> event = eventSerializer.deserialize(record.value());
                            events.add(event);
                            lastEventSeq = event.getSequenceNumber();
                            found = true;
                            
                            log.debug("📥 Evento #{} encontrado", event.getSequenceNumber());
                            
                            // MEJORA 4: Ajustar timeout si se encontraron eventos
                            if (events.size() == 1) {
                                timeoutMillis = calculateTimeout(true);
                            }
                                
                        } catch (Exception e) {
                            log.error("Error deserializando evento", e);
                        }
                    }
                }
            }
            
            long searchTimeMs = System.currentTimeMillis() - startTime;
            
            if (events.isEmpty()) {
                log.info("⚠️ No se encontraron eventos para: {} (búsqueda: {}ms)", 
                    aggregateIdentifier, searchTimeMs);
                return;
            }
            
            // Ordenar y persistir eventos
            events.sort(Comparator.comparingLong(DomainEventMessage::getSequenceNumber));
            
            log.info("📦 Persistiendo {} eventos (seq: 0-{}) - Búsqueda: {}ms", 
                events.size(), lastEventSeq, searchTimeMs);
            
            persistEvents(em, events);
            
            markAsMaterialized(aggregateIdentifier);
            
            long totalTimeMs = System.currentTimeMillis() - overallStartTime;
            updateMetrics(totalTimeMs);
            
            log.info("✅ Aggregate {} materializado: {} eventos en {}ms", 
                aggregateIdentifier, events.size(), totalTimeMs);
            
        } catch (Exception e) {
            log.error("💥 Error materializando desde Kafka", e);
            throw new RuntimeException("Error en materialización", e);
            
        } finally {
            if (consumer != null) {
                returnConsumerToPool(consumer);
            }
        }
    }
    
    /**
     * MEJORA: Detecta si el consumer ha alcanzado el final del topic en todas las particiones.
     */
    private boolean hasReachedEnd(
            KafkaConsumer<String, String> consumer,
            List<TopicPartition> partitions,
            Map<TopicPartition, Long> endOffsets) {
        
        try {
            for (TopicPartition partition : partitions) {
                long currentPosition = consumer.position(partition);
                Long endOffset = endOffsets.get(partition);
                
                if (endOffset != null && currentPosition < endOffset) {
                    // Aún hay mensajes por leer en esta partición
                    return false;
                }
            }
            
            // Todas las particiones alcanzaron el final
            return true;
            
        } catch (Exception e) {
            log.warn("Error verificando end offsets: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * MEJORA: Calcula timeout adaptativo basado en el contexto.
     * 
     * - Si NO se encontraron eventos: timeout corto (2s)
     * - Si SÍ se encontraron eventos: timeout más largo (5s) para leer todos
     */
    private long calculateTimeout(boolean found) {
        if (found) {
            // Si ya encontramos eventos, dar más tiempo para leer el resto
            return 5000; // 5 segundos
        } else {
            // Si no hay eventos, usar timeout corto
            long configuredTimeout = properties.getMaterializer().getTimeoutSeconds() * 1000;
            return Math.min(configuredTimeout, 2000); // Máximo 2 segundos
        }
    }
    
    /**
     * Persiste eventos en lotes.
     */
    private void persistEvents(EntityManager em, List<DomainEventMessage<?>> events) {
        int batchSize = 50;
        
        for (int i = 0; i < events.size(); i++) {
            DomainEventMessage<?> event = events.get(i);
            
            Long count = em.createQuery(
                "SELECT COUNT(e) FROM DomainEventEntry e " +
                "WHERE e.aggregateIdentifier = :aggId AND e.sequenceNumber = :seq",
                Long.class)
                .setParameter("aggId", event.getAggregateIdentifier())
                .setParameter("seq", event.getSequenceNumber())
                .getSingleResult();
            
            if (count == 0) {
                DomainEventEntry entry = new DomainEventEntry(event, axonSerializer);
                em.persist(entry);
            }
            
            if (i > 0 && i % batchSize == 0) {
                em.flush();
                em.clear();
                log.debug("✅ Batch {} persistido", i/batchSize);
            }
        }
        
        em.flush();
    }
    
    /**
     * Actualiza métricas de rendimiento.
     */
    private void updateMetrics(long timeMs) {
        synchronized (this) {
            totalMaterializationTimeMs += timeMs;
            materializationCount++;
        }
    }

    private KafkaConsumer<String, String> getConsumerFromPool() {
        synchronized (consumerPool) {
            if (!consumerPool.isEmpty()) {
                log.debug("♻️ Reutilizando consumer");
                return consumerPool.poll();
            }
        }
        
        log.debug("🆕 Creando nuevo consumer");
        return createConsumer();
    }

    private void returnConsumerToPool(KafkaConsumer<String, String> consumer) {
        synchronized (consumerPool) {
            int maxPoolSize = properties.getMaterializer().getConsumerPoolSize();
            if (consumerPool.size() < maxPoolSize) {
                consumerPool.offer(consumer);
                log.debug("♻️ Consumer devuelto al pool");
            } else {
                consumer.close();
                log.debug("🗑️ Consumer cerrado");
            }
        }
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", properties.getBootstrapServers());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "event-materializer-" + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "500");
        
        return new KafkaConsumer<>(props);
    }

    public Map<String, Object> getStats() {
        synchronized (consumerPool) {
            long avgTimeMs = materializationCount > 0 
                ? totalMaterializationTimeMs / materializationCount 
                : 0;
            
            return Map.of(
                "cachedAggregates", materializedCache.size(),
                "consumerPoolSize", consumerPool.size(),
                "maxPoolSize", properties.getMaterializer().getConsumerPoolSize(),
                "totalMaterializations", materializationCount,
                "avgMaterializationTimeMs", avgTimeMs
            );
        }
    }

    public void clearCache() {
        materializedCache.clear();
        log.info("🗑️ Cache limpiado");
    }

    public void shutdown() {
        synchronized (consumerPool) {
            while (!consumerPool.isEmpty()) {
                KafkaConsumer<String, String> consumer = consumerPool.poll();
                consumer.close();
            }
            log.info("🛑 Consumers cerrados");
        }
    }
}