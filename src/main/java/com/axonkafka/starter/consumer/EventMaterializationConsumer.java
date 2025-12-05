package com.axonkafka.starter.consumer;

import com.axonkafka.starter.serializer.KafkaEventSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventsourcing.eventstore.jpa.DomainEventEntry;
import org.axonframework.serialization.Serializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "axon.kafka.materializer", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EventMaterializationConsumer {

    private final EntityManagerProvider entityManagerProvider;
    private final KafkaEventSerializer eventSerializer;
    private final Serializer axonSerializer;

    public EventMaterializationConsumer(
            EntityManagerProvider entityManagerProvider,
            KafkaEventSerializer eventSerializer,
            Serializer defaultSerializer) {
        this.entityManagerProvider = entityManagerProvider;
        this.eventSerializer = eventSerializer;
        this.axonSerializer = defaultSerializer;
    }

    @KafkaListener(
            topics = "${axon.kafka.event.topic}",
            groupId = "${axon.kafka.event.materializer-group-id}",
            concurrency = "${axon.kafka.materializer.concurrency:5}",
            containerFactory = "materializerKafkaListenerContainerFactory"
    )
    @Transactional
    public void materializeEvent(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        try {
            log.debug("Materializando evento - Offset: {}, Partition: {}",
                    record.offset(), record.partition());

            DomainEventMessage<?> event = eventSerializer.deserialize(record.value());

            EntityManager em = entityManagerProvider.getEntityManager();

            Long count = em.createQuery(
                            "SELECT COUNT(e) FROM DomainEventEntry e " +
                                    "WHERE e.aggregateIdentifier = :aggId AND e.sequenceNumber = :seq",
                            Long.class)
                    .setParameter("aggId", event.getAggregateIdentifier())
                    .setParameter("seq", event.getSequenceNumber())
                    .getSingleResult();

            if (count > 0) {
                log.debug("Evento ya materializado (idempotente): {} seq={}",
                        event.getAggregateIdentifier(), event.getSequenceNumber());
                acknowledgment.acknowledge();
                return;
            }

            DomainEventEntry entry = new DomainEventEntry(event, axonSerializer);
            em.persist(entry);
            em.flush();

            log.debug("Evento materializado en PG: aggregate={}, seq={}",
                    event.getAggregateIdentifier(), event.getSequenceNumber());

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Error materializando evento - NO se hace acknowledge para reintentar", e);
            throw new RuntimeException("Error en materialización", e);
        }
    }
}