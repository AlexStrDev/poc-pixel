package io.github.axonkafka.topic;

import io.github.axonkafka.properties.AxonKafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Componente que crea automáticamente los topics de Kafka necesarios
 * para el funcionamiento del Axon Kafka Starter.
 * 
 * Se ejecuta después de que la aplicación esté lista (ApplicationReadyEvent).
 */
@Slf4j
@Component
public class KafkaTopicCreator {

    private final AxonKafkaProperties properties;
    private AdminClient adminClient;

    public KafkaTopicCreator(AxonKafkaProperties properties) {
        this.properties = properties;
    }

    /**
     * Crea los topics necesarios cuando la aplicación está lista.
     * 
     * Topics creados:
     * - command.topic: Para enviar comandos
     * - command.reply-topic: Para recibir respuestas de comandos
     * - command.dlq-topic: Dead Letter Queue para comandos fallidos
     * - event.topic: Para publicar y consumir eventos
     */
    @EventListener(ApplicationReadyEvent.class)
    public void createTopics() {
        log.info("🔧 Verificando y creando topics de Kafka necesarios...");

        try {
            // Crear AdminClient
            adminClient = createAdminClient();

            List<String> topicsToCreate = Arrays.asList(
                properties.getCommand().getTopic(),
                properties.getCommand().getReplyTopic(),
                properties.getCommand().getDlqTopic(),
                properties.getEvent().getTopic()
            );

            for (String topicName : topicsToCreate) {
                createTopic(topicName);
            }

            log.info("✅ Verificación de topics completada");

        } catch (Exception e) {
            log.error("❌ Error creando topics de Kafka", e);
            // NO lanzar excepción aquí, solo logear el error
            // La aplicación puede continuar aunque fallen los topics
            log.warn("⚠️ La aplicación continuará, pero algunos topics pueden no existir");
        }
    }

    /**
     * Crea un AdminClient para interactuar con Kafka.
     */
    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        return AdminClient.create(props);
    }

    /**
     * Crea un topic individual con la configuración especificada.
     * Si el topic ya existe, simplemente lo ignora y continúa.
     * 
     * @param topicName Nombre del topic a crear
     */
    private void createTopic(String topicName) {
        try {
            log.info("📝 Creando topic '{}' con {} particiones...", 
                topicName, properties.getTopic().getPartitions());

            // Configuración del topic
            Map<String, String> configs = new HashMap<>();
            configs.put("retention.ms", 
                String.valueOf(properties.getTopic().getRetentionHours() * 3600000L));
            configs.put("cleanup.policy", "delete");

            // Crear nuevo topic
            NewTopic newTopic = new NewTopic(
                topicName,
                properties.getTopic().getPartitions(),
                (short) properties.getTopic().getReplicationFactor()
            );
            newTopic.configs(configs);

            // Enviar solicitud de creación
            CreateTopicsResult result = adminClient.createTopics(Arrays.asList(newTopic));

            // Esperar a que se complete (bloqueante)
            result.values().get(topicName).get();

            log.info("✅ Topic '{}' creado exitosamente", topicName);

        } catch (ExecutionException e) {
            // Verificar si es porque el topic ya existe
            if (e.getCause() instanceof TopicExistsException) {
                log.info("ℹ️ Topic '{}' ya existe, continuando...", topicName);
            } else {
                log.error("❌ Error creando topic '{}'", topicName, e);
                // NO lanzar excepción, solo logear
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("❌ Creación de topic '{}' interrumpida", topicName, e);
        } catch (Exception e) {
            log.error("❌ Error inesperado creando topic '{}'", topicName, e);
        }
    }

    /**
     * Cierra el AdminClient cuando el bean se destruye.
     */
    @PreDestroy
    public void destroy() {
        if (adminClient != null) {
            try {
                adminClient.close();
                log.debug("AdminClient cerrado correctamente");
            } catch (Exception e) {
                log.warn("Error cerrando AdminClient", e);
            }
        }
    }
}