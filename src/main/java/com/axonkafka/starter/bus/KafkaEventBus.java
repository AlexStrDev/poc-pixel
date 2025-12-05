package com.axonkafka.starter.bus;

import com.axonkafka.starter.properties.AxonKafkaProperties;
import com.axonkafka.starter.serializer.KafkaEventSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.eventhandling.DomainEventMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@Slf4j
@Component
public class KafkaEventBus {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaEventSerializer kafkaEventSerializer;
    private final String eventTopic;
    private final List<Consumer<DomainEventMessage<?>>> eventHandlers;

    public KafkaEventBus(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaEventSerializer kafkaEventSerializer,
            AxonKafkaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaEventSerializer = kafkaEventSerializer;
        this.eventTopic = properties.getEvent().getTopic();
        this.eventHandlers = new CopyOnWriteArrayList<>();
    }

    public void publish(DomainEventMessage<?> event) {
        try {
            log.debug("Publicando evento: {} para agregado: {}",
                    event.getPayloadType().getSimpleName(),
                    event.getAggregateIdentifier());

            String serialized = kafkaEventSerializer.serialize(event);
            kafkaTemplate.send(eventTopic, event.getAggregateIdentifier(), serialized);

        } catch (Exception e) {
            log.error("Error publicando evento a Kafka", e);
            throw new RuntimeException("Error publicando evento", e);
        }
    }

    @KafkaListener(
            topics = "${axon.kafka.event.topic}",
            groupId = "${axon.kafka.event.group-id}",
            containerFactory = "eventKafkaListenerContainerFactory"
    )
    public void consumeEvent(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.debug("Evento recibido - Offset: {}", record.offset());

            DomainEventMessage<?> event = kafkaEventSerializer.deserialize(record.value());

            eventHandlers.forEach(handler -> {
                try {
                    handler.accept(event);
                } catch (Exception e) {
                    log.error("Error en handler de evento", e);
                }
            });

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }

        } catch (Exception e) {
            log.error("Error consumiendo evento de Kafka", e);
        }
    }

    public void registerEventHandler(Consumer<DomainEventMessage<?>> handler) {
        eventHandlers.add(handler);
    }
}