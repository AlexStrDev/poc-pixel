package io.github.axonkafka.topic;

import io.github.axonkafka.properties.AxonKafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
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
 * CARACTERÍSTICAS:
 * - Crea topics con la configuración especificada
 * - Verifica topics existentes
 * - AUTO-CORRIGE particiones cuando hay mismatch
 * - Logging detallado
 */
@Slf4j
@Component
public class KafkaTopicCreator {

    private final AxonKafkaProperties properties;
    private AdminClient adminClient;

    public KafkaTopicCreator(AxonKafkaProperties properties) {
        this.properties = properties;
        log.info("🔧 KafkaTopicCreator inicializado");
        log.info("   📋 Configuración cargada:");
        log.info("      - Bootstrap Servers: {}", properties.getBootstrapServers());
        log.info("      - Particiones configuradas: {}", properties.getTopic().getPartitions());
        log.info("      - Factor de replicación: {}", properties.getTopic().getReplicationFactor());
        log.info("      - Retención (horas): {}", properties.getTopic().getRetentionHours());
        log.info("      - Auto-corrección de particiones: {}", 
            properties.getTopic().isAutoCorrectPartitions() ? "ACTIVADA ✅" : "DESACTIVADA ❌");
    }

    /**
     * Crea los topics necesarios cuando la aplicación está lista.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void createTopics() {
        log.info("🚀 Iniciando verificación y creación de topics de Kafka...");
        log.info("═══════════════════════════════════════════════════════════");

        try {
            // Crear AdminClient
            adminClient = createAdminClient();

            List<String> topicsToCreate = Arrays.asList(
                properties.getCommand().getTopic(),
                properties.getCommand().getReplyTopic(),
                properties.getCommand().getDlqTopic(),
                properties.getEvent().getTopic()
            );

            log.info("📝 Topics a verificar/crear:");
            topicsToCreate.forEach(topic -> log.info("   - {}", topic));
            log.info("═══════════════════════════════════════════════════════════");

            boolean anyTopicCorrected = false;

            for (String topicName : topicsToCreate) {
                boolean corrected = createOrUpdateTopic(topicName);
                if (corrected) {
                    anyTopicCorrected = true;
                }
            }

            log.info("═══════════════════════════════════════════════════════════");
            
            if (anyTopicCorrected) {
                log.info("✅ Verificación completada - Se corrigieron particiones automáticamente");
            } else {
                log.info("✅ Verificación completada - Todos los topics están correctos");
            }

        } catch (Exception e) {
            log.error("═══════════════════════════════════════════════════════════");
            log.error("❌ Error creando topics de Kafka", e);
            log.warn("⚠️ La aplicación continuará, pero algunos topics pueden tener problemas");
        }
    }

    /**
     * Crea un AdminClient para interactuar con Kafka.
     */
    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
        return AdminClient.create(props);
    }

    /**
     * Crea o actualiza un topic con la configuración especificada.
     * 
     * @param topicName Nombre del topic a crear/actualizar
     * @return true si se corrigieron particiones, false en caso contrario
     */
    private boolean createOrUpdateTopic(String topicName) {
        try {
            int desiredPartitions = properties.getTopic().getPartitions();
            short desiredReplication = properties.getTopic().getReplicationFactor();
            
            log.info("───────────────────────────────────────────────────────────");
            log.info("📝 Procesando topic: '{}'", topicName);
            log.info("   Particiones deseadas: {}", desiredPartitions);
            log.info("   Replicación deseada: {}", desiredReplication);
            
            // Verificar si el topic existe
            if (topicExists(topicName)) {
                log.info("ℹ️ El topic '{}' ya existe, verificando configuración...", topicName);
                return verifyAndCorrectPartitions(topicName, desiredPartitions);
            }

            // Topic no existe, crear nuevo
            log.info("🆕 Creando nuevo topic '{}'...", topicName);
            createNewTopic(topicName, desiredPartitions, desiredReplication);
            
            // Verificar que se creó correctamente
            verifyTopicConfiguration(topicName, desiredPartitions, false);
            return false;

        } catch (Exception e) {
            log.error("❌ Error procesando topic '{}'", topicName, e);
            return false;
        }
    }

    /**
     * Crea un nuevo topic.
     */
    private void createNewTopic(String topicName, int partitions, short replication) throws Exception {
        // Configuración del topic
        Map<String, String> configs = new HashMap<>();
        configs.put("retention.ms", 
            String.valueOf(properties.getTopic().getRetentionHours() * 3600000L));
        configs.put("cleanup.policy", "delete");

        // Crear nuevo topic
        NewTopic newTopic = new NewTopic(topicName, partitions, replication);
        newTopic.configs(configs);

        try {
            // Enviar solicitud de creación
            CreateTopicsResult result = adminClient.createTopics(Arrays.asList(newTopic));
            result.values().get(topicName).get();
            
            log.info("✅ Topic '{}' creado exitosamente", topicName);
            
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.info("ℹ️ Topic '{}' ya existe (race condition)", topicName);
            } else {
                throw e;
            }
        }
    }
    
    /**
     * Verifica si un topic existe.
     */
    private boolean topicExists(String topicName) {
        try {
            return adminClient.listTopics().names().get().contains(topicName);
        } catch (Exception e) {
            log.warn("⚠️ No se pudo verificar si el topic '{}' existe: {}", topicName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Verifica y corrige automáticamente el número de particiones si hay mismatch.
     * 
     * @return true si se corrigieron particiones, false en caso contrario
     */
    private boolean verifyAndCorrectPartitions(String topicName, int desiredPartitions) {
        try {
            var topicDescription = adminClient.describeTopics(Arrays.asList(topicName))
                .allTopicNames()
                .get()
                .get(topicName);
            
            int actualPartitions = topicDescription.partitions().size();
            
            log.info("📊 Configuración actual:");
            log.info("   - Particiones actuales: {}", actualPartitions);
            log.info("   - Particiones deseadas: {}", desiredPartitions);
            
            if (actualPartitions == desiredPartitions) {
                log.info("✅ Topic '{}' correcto: {} particiones", topicName, actualPartitions);
                return false;
            }
            
            if (actualPartitions > desiredPartitions) {
                log.warn("⚠️ Topic '{}' tiene {} particiones (más de las deseadas: {})", 
                    topicName, actualPartitions, desiredPartitions);
                log.warn("⚠️ No se pueden reducir particiones en Kafka");
                log.warn("⚠️ Considera ajustar: axon.kafka.topic.partitions={}", actualPartitions);
                return false;
            }
            
            // actualPartitions < desiredPartitions
            if (!properties.getTopic().isAutoCorrectPartitions()) {
                log.warn("═══════════════════════════════════════════════════════════");
                log.warn("⚠️ ⚠️ ⚠️  CONFIGURACIÓN INCORRECTA  ⚠️ ⚠️ ⚠️");
                log.warn("═══════════════════════════════════════════════════════════");
                log.warn("⚠️ Topic '{}' tiene {} particiones pero se esperaban {}", 
                    topicName, actualPartitions, desiredPartitions);
                log.warn("");
                log.warn("🔧 OPCIONES PARA CORREGIR:");
                log.warn("");
                log.warn("1️⃣ HABILITAR AUTO-CORRECCIÓN (recomendado):");
                log.warn("   En application.properties agrega:");
                log.warn("   axon.kafka.topic.auto-correct-partitions=true");
                log.warn("   Y reinicia la aplicación");
                log.warn("");
                log.warn("2️⃣ INCREMENTAR MANUALMENTE:");
                log.warn("   kafka-topics.sh --alter --topic {} \\", topicName);
                log.warn("     --partitions {} \\", desiredPartitions);
                log.warn("     --bootstrap-server {}", properties.getBootstrapServers());
                log.warn("");
                log.warn("3️⃣ ELIMINAR Y RECREAR (solo desarrollo):");
                log.warn("   kafka-topics.sh --delete --topic {} \\", topicName);
                log.warn("     --bootstrap-server {}", properties.getBootstrapServers());
                log.warn("   Luego reinicia la aplicación");
                log.warn("═══════════════════════════════════════════════════════════");
                return false;
            }
            
            // Auto-corrección habilitada - INCREMENTAR AUTOMÁTICAMENTE
            log.info("🔧 CORRIGIENDO: Incrementando particiones de {} a {}", 
                actualPartitions, desiredPartitions);
            
            return incrementPartitions(topicName, desiredPartitions);
            
        } catch (Exception e) {
            log.error("⚠️ Error verificando configuración del topic '{}': {}", 
                topicName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Incrementa el número de particiones de un topic existente.
     * 
     * @return true si se incrementaron correctamente, false en caso contrario
     */
    private boolean incrementPartitions(String topicName, int newTotalPartitions) {
        try {
            log.info("   ⏳ Incrementando particiones del topic '{}'...", topicName);
            
            // Crear solicitud de incremento
            Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
            newPartitionsMap.put(topicName, NewPartitions.increaseTo(newTotalPartitions));
            
            // Aplicar cambios
            CreatePartitionsOptions options = new CreatePartitionsOptions();
            options.timeoutMs(10000);
            
            adminClient.createPartitions(newPartitionsMap, options).all().get();
            
            log.info("   ✅ Particiones incrementadas exitosamente");
            
            // Verificar el resultado
            Thread.sleep(1000); // Esperar un momento para que Kafka aplique los cambios
            verifyTopicConfiguration(topicName, newTotalPartitions, true);
            
            return true;
            
        } catch (Exception e) {
            log.error("   ❌ Error incrementando particiones: {}", e.getMessage());
            
            log.warn("═══════════════════════════════════════════════════════════");
            log.warn("⚠️ No se pudo incrementar automáticamente");
            log.warn("🔧 Intenta manualmente:");
            log.warn("   kafka-topics.sh --alter --topic {} \\", topicName);
            log.warn("     --partitions {} \\", newTotalPartitions);
            log.warn("     --bootstrap-server {}", properties.getBootstrapServers());
            log.warn("═══════════════════════════════════════════════════════════");
            
            return false;
        }
    }
    
    /**
     * Verifica la configuración final de un topic.
     */
    private void verifyTopicConfiguration(String topicName, int expectedPartitions, boolean afterCorrection) {
        try {
            var topicDescription = adminClient.describeTopics(Arrays.asList(topicName))
                .allTopicNames()
                .get()
                .get(topicName);
            
            int actualPartitions = topicDescription.partitions().size();
            
            if (actualPartitions == expectedPartitions) {
                if (afterCorrection) {
                    log.info("   ✅ Verificado: {} ahora tiene {} particiones", 
                        topicName, actualPartitions);
                }
            } else {
                log.warn("   ⚠️ Verificación falló: {} tiene {} particiones (se esperaban {})", 
                    topicName, actualPartitions, expectedPartitions);
            }
            
        } catch (Exception e) {
            log.warn("   ⚠️ No se pudo verificar la configuración final: {}", e.getMessage());
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