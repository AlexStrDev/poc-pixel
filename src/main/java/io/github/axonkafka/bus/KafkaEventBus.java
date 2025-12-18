package io.github.axonkafka.bus;

import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.KafkaEventSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.axonframework.eventhandling.DomainEventMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * EventBus genérico usando Kafka como transport.
 */
@Slf4j
public class KafkaEventBus {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaEventSerializer eventSerializer;
    private final AxonKafkaProperties properties;
    private final List<Consumer<DomainEventMessage<?>>> eventHandlers;

    public KafkaEventBus(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaEventSerializer eventSerializer,
            AxonKafkaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventSerializer = eventSerializer;
        this.properties = properties;
        this.eventHandlers = new CopyOnWriteArrayList<>();
    }

    public void publish(DomainEventMessage<?> event) {
        try {
            log.debug("Publicando evento: {} para agregado: {}", 
                event.getPayloadType().getSimpleName(), 
                event.getAggregateIdentifier());
            
            String serialized = eventSerializer.serialize(event);
            String topic = properties.getEvent().getTopic();
            kafkaTemplate.send(topic, event.getAggregateIdentifier(), serialized);
            
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
            
            DomainEventMessage<?> event = eventSerializer.deserialize(record.value());
            
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
            log.error("Error consumiendo evento", e);
        }
    }

    public void registerEventHandler(Consumer<DomainEventMessage<?>> handler) {
        eventHandlers.add(handler);
    }
}