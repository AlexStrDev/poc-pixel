package io.github.axonkafka.storage;

import io.github.axonkafka.properties.AxonKafkaProperties;
import io.github.axonkafka.serializer.KafkaEventSerializer;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests unitarios para EventStoreMaterializer
 * 
 * Cubre:
 * - Verificación de materialización
 * - Marcado como materializado
 * - Remoción de marca (FIX de re-materialización)
 * - Cache en memoria
 * - Limpieza de cache
 * - Métricas
 * - Pool de consumers
 */
class EventStoreMaterializerTest {

    private EventStoreMaterializer materializer;
    private EntityManagerProvider entityManagerProvider;
    private KafkaEventSerializer eventSerializer;
    private Serializer axonSerializer;
    private AxonKafkaProperties properties;
    private EntityManager entityManager;

    static class TestEvent {
        private String data;
        public TestEvent() {}
        public TestEvent(String data) { this.data = data; }
        public String getData() { return data; }
        public void setData(String data) { this.data = data; }
    }

    @BeforeEach
    void setUp() {
        entityManagerProvider = mock(EntityManagerProvider.class);
        eventSerializer = mock(KafkaEventSerializer.class);
        axonSerializer = mock(Serializer.class);
        entityManager = mock(EntityManager.class);
        
        when(entityManagerProvider.getEntityManager()).thenReturn(entityManager);
        
        properties = new AxonKafkaProperties();
        properties.setBootstrapServers("localhost:9092");
        properties.getEvent().setTopic("test-events");
        properties.getMaterializer().setGroupId("test-materializer");
        properties.getMaterializer().setConcurrency(3);
        properties.getMaterializer().setTimeoutSeconds(30);
        properties.getMaterializer().setConsumerPoolSize(5);

        materializer = new EventStoreMaterializer(
            entityManagerProvider,
            eventSerializer,
            axonSerializer,
            properties
        );
    }

    @Test
    @DisplayName("Debe retornar false si aggregate no está materializado")
    void testIsNotMaterialized() {
        // Given
        String aggregateId = "AGG-001";
        
        TypedQuery<Long> query = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(query);
        when(query.setParameter(anyString(), any())).thenReturn(query);
        when(query.setMaxResults(anyInt())).thenReturn(query);
        when(query.getSingleResult()).thenReturn(0L);

        // When
        boolean materialized = materializer.isMaterialized(aggregateId);

        // Then
        assertThat(materialized).isFalse();
    }

    @Test
    @DisplayName("Debe retornar true si aggregate está materializado en PG")
    void testIsMaterializedInDatabase() {
        // Given
        String aggregateId = "AGG-002";
        
        TypedQuery<Long> query = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(query);
        when(query.setParameter(anyString(), any())).thenReturn(query);
        when(query.setMaxResults(anyInt())).thenReturn(query);
        when(query.getSingleResult()).thenReturn(5L); // 5 eventos en PG

        // When
        boolean materialized = materializer.isMaterialized(aggregateId);

        // Then
        assertThat(materialized).isTrue();
    }

    @Test
    @DisplayName("Debe usar cache si aggregate ya está marcado como materializado")
    void testCacheHit() {
        // Given
        String aggregateId = "AGG-003";
        materializer.markAsMaterialized(aggregateId);

        // When
        boolean materialized = materializer.isMaterialized(aggregateId);

        // Then
        assertThat(materialized).isTrue();
        verify(entityManager, never()).createQuery(anyString(), any()); // No consulta a BD
    }

    @Test
    @DisplayName("Debe marcar aggregate como materializado correctamente")
    void testMarkAsMaterialized() {
        // Given
        String aggregateId = "AGG-004";

        // When
        materializer.markAsMaterialized(aggregateId);
        boolean materialized = materializer.isMaterialized(aggregateId);

        // Then
        assertThat(materialized).isTrue();
    }

    @Test
    @DisplayName("FIX: Debe remover marca de materialización cuando se detecta que PG está vacío")
    void testRemoveMaterializedMark() {
        // Given
        String aggregateId = "AGG-005";
        materializer.markAsMaterialized(aggregateId);
        
        // Verificar que está marcado
        assertThat(materializer.isMaterialized(aggregateId)).isTrue();

        // When - Remover marca (simulando detección de PG vacío)
        materializer.removeMaterializedMark(aggregateId);

        // Then
        // Ahora debe consultar a PG porque el cache fue limpiado
        TypedQuery<Long> query = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(query);
        when(query.setParameter(anyString(), any())).thenReturn(query);
        when(query.setMaxResults(anyInt())).thenReturn(query);
        when(query.getSingleResult()).thenReturn(0L);
        
        boolean materialized = materializer.isMaterialized(aggregateId);
        assertThat(materialized).isFalse();
    }

    @Test
    @DisplayName("FIX: Debe permitir re-materialización después de remover marca")
    void testRematerializationAfterMarkRemoval() {
        // Given
        String aggregateId = "AGG-006";
        
        // Paso 1: Marcar como materializado
        materializer.markAsMaterialized(aggregateId);
        assertThat(materializer.isMaterialized(aggregateId)).isTrue();
        
        // Paso 2: Simular que se detectó PG vacío y se removió marca
        materializer.removeMaterializedMark(aggregateId);
        
        // Configurar mock para que PG esté vacío
        TypedQuery<Long> query = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(query);
        when(query.setParameter(anyString(), any())).thenReturn(query);
        when(query.setMaxResults(anyInt())).thenReturn(query);
        when(query.getSingleResult()).thenReturn(0L);

        // When
        boolean materialized = materializer.isMaterialized(aggregateId);

        // Then
        assertThat(materialized).isFalse(); // Ahora detecta que debe re-materializar
    }

    @Test
    @DisplayName("Debe limpiar todo el cache correctamente")
    void testClearCache() {
        // Given
        materializer.markAsMaterialized("AGG-007");
        materializer.markAsMaterialized("AGG-008");
        materializer.markAsMaterialized("AGG-009");
        
        Map<String, Object> statsBefore = materializer.getStats();
        assertThat(statsBefore.get("cachedAggregates")).isEqualTo(3);

        // When
        materializer.clearCache();

        // Then
        Map<String, Object> statsAfter = materializer.getStats();
        assertThat(statsAfter.get("cachedAggregates")).isEqualTo(0);
    }

    @Test
    @DisplayName("Debe retornar estadísticas correctas")
    void testGetStats() {
        // Given
        materializer.markAsMaterialized("AGG-010");
        materializer.markAsMaterialized("AGG-011");

        // When
        Map<String, Object> stats = materializer.getStats();

        // Then
        assertThat(stats).containsKeys(
            "cachedAggregates",
            "consumerPoolSize",
            "maxPoolSize",
            "totalMaterializations",
            "avgMaterializationTimeMs"
        );
        assertThat(stats.get("cachedAggregates")).isEqualTo(2);
        assertThat(stats.get("maxPoolSize")).isEqualTo(5);
    }

    @Test
    @DisplayName("Debe manejar múltiples aggregates en cache")
    void testMultipleAggregatesInCache() {
        // Given
        int count = 100;
        for (int i = 0; i < count; i++) {
            materializer.markAsMaterialized("AGG-" + i);
        }

        // When
        Map<String, Object> stats = materializer.getStats();

        // Then
        assertThat(stats.get("cachedAggregates")).isEqualTo(count);
    }

    @Test
    @DisplayName("Debe manejar errores al consultar PostgreSQL")
    void testHandleDatabaseError() {
        // Given
        String aggregateId = "AGG-012";
        
        when(entityManager.createQuery(anyString(), eq(Long.class)))
            .thenThrow(new RuntimeException("Database error"));

        // When
        boolean materialized = materializer.isMaterialized(aggregateId);

        // Then
        assertThat(materialized).isFalse(); // Debe retornar false en caso de error
    }

    @Test
    @DisplayName("Debe verificar aggregate correctamente cuando cache y BD están sincronizados")
    void testCacheAndDatabaseSynchronized() {
        // Given
        String aggregateId = "AGG-013";
        
        // Configurar PG con eventos
        TypedQuery<Long> query = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(query);
        when(query.setParameter(anyString(), any())).thenReturn(query);
        when(query.setMaxResults(anyInt())).thenReturn(query);
        when(query.getSingleResult()).thenReturn(10L);

        // When - Primera llamada consulta BD y cachea
        boolean first = materializer.isMaterialized(aggregateId);
        
        // Segunda llamada usa cache
        boolean second = materializer.isMaterialized(aggregateId);

        // Then
        assertThat(first).isTrue();
        assertThat(second).isTrue();
        verify(entityManager, times(1)).createQuery(anyString(), eq(Long.class)); // Solo 1 consulta
    }

    @Test
    @DisplayName("FIX: Debe detectar inconsistencia cache-BD y permitir re-materialización")
    void testDetectCacheDatabaseInconsistency() {
        // Given - Escenario: Cache dice "materializado" pero PG está vacío
        String aggregateId = "AGG-014";
        
        // Paso 1: Marcar en cache (simula que estuvo materializado antes)
        materializer.markAsMaterialized(aggregateId);
        
        // Paso 2: Simular que alguien borró los datos de PG
        // (Esto es lo que el usuario hizo en su test que falló)
        
        // Paso 3: El fix debe permitir remover la marca y re-materializar
        materializer.removeMaterializedMark(aggregateId);
        
        // Configurar PG vacío
        TypedQuery<Long> query = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(query);
        when(query.setParameter(anyString(), any())).thenReturn(query);
        when(query.setMaxResults(anyInt())).thenReturn(query);
        when(query.getSingleResult()).thenReturn(0L);

        // When
        boolean materialized = materializer.isMaterialized(aggregateId);

        // Then
        assertThat(materialized).isFalse(); // ✅ FIX: Ahora detecta que debe re-materializar
    }

    @Test
    @DisplayName("Debe manejar aggregates con IDs largos")
    void testLongAggregateIds() {
        // Given
        String longId = "AGG-" + "A".repeat(1000);

        // When
        materializer.markAsMaterialized(longId);
        boolean materialized = materializer.isMaterialized(longId);

        // Then
        assertThat(materialized).isTrue();
    }

    @Test
    @DisplayName("Debe manejar aggregates con caracteres especiales")
    void testSpecialCharactersInAggregateIds() {
        // Given
        String[] specialIds = {
            "AGG:001",
            "AGG/002",
            "AGG-003",
            "AGG_004",
            "AGG.005"
        };

        // When / Then
        for (String id : specialIds) {
            materializer.markAsMaterialized(id);
            assertThat(materializer.isMaterialized(id)).isTrue();
        }
    }

    @Test
    @DisplayName("Debe ser thread-safe al marcar como materializado")
    void testThreadSafeMarkAsMaterialized() throws InterruptedException {
        // Given
        String aggregateId = "AGG-CONCURRENT";
        int threadCount = 50;
        CountDownLatch latch = new CountDownLatch(threadCount);

        // When
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    materializer.markAsMaterialized(aggregateId);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        // Then
        assertThat(materializer.isMaterialized(aggregateId)).isTrue();
        Map<String, Object> stats = materializer.getStats();
        assertThat(stats.get("cachedAggregates")).isEqualTo(1); // Solo una entrada
    }

    @Test
    @DisplayName("Debe actualizar métricas de materialización")
    void testMaterializationMetrics() {
        // Given / When
        // Las métricas se actualizan en materializeFromKafka()
        // que es un método complejo que requiere Kafka real
        // Aquí solo verificamos que getStats() retorna las claves correctas
        
        Map<String, Object> stats = materializer.getStats();

        // Then
        assertThat(stats).containsKeys(
            "cachedAggregates",
            "consumerPoolSize",
            "maxPoolSize",
            "totalMaterializations",
            "avgMaterializationTimeMs"
        );
    }

    @Test
    @DisplayName("Debe mantener cache consistente después de operaciones concurrentes")
    void testCacheConsistencyUnderConcurrency() throws InterruptedException {
        // Given
        int operationCount = 100;
        CountDownLatch latch = new CountDownLatch(operationCount);

        // When - Operaciones concurrentes de marcar y verificar
        for (int i = 0; i < operationCount; i++) {
            final int index = i;
            new Thread(() -> {
                try {
                    String aggregateId = "AGG-" + (index % 10); // 10 aggregates diferentes
                    materializer.markAsMaterialized(aggregateId);
                    materializer.isMaterialized(aggregateId);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();

        // Then
        Map<String, Object> stats = materializer.getStats();
        assertThat(stats.get("cachedAggregates")).isEqualTo(10); // 10 aggregates únicos
    }

    @Test
    @DisplayName("Debe limpiar cache y permitir nueva consulta a BD")
    void testClearCacheForcesDatabaseQuery() {
        // Given
        String aggregateId = "AGG-015";
        
        TypedQuery<Long> query = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(query);
        when(query.setParameter(anyString(), any())).thenReturn(query);
        when(query.setMaxResults(anyInt())).thenReturn(query);
        when(query.getSingleResult()).thenReturn(5L);
        
        // Primera consulta
        materializer.isMaterialized(aggregateId);
        verify(entityManager, times(1)).createQuery(anyString(), eq(Long.class));
        
        // Segunda consulta usa cache
        materializer.isMaterialized(aggregateId);
        verify(entityManager, times(1)).createQuery(anyString(), eq(Long.class)); // Aún 1

        // When - Limpiar cache
        materializer.clearCache();
        
        // Tercera consulta después de limpiar cache
        materializer.isMaterialized(aggregateId);

        // Then - Debe haber consultado BD nuevamente
        verify(entityManager, times(2)).createQuery(anyString(), eq(Long.class));
    }

    @Test
    @DisplayName("FIX: Debe soportar flujo completo de re-materialización")
    void testCompleteRematerializationFlow() {
        // Given - Escenario del bug original del usuario
        String aggregateId = "ORDER-001";
        
        // Paso 1: Estado inicial - aggregate materializado
        TypedQuery<Long> queryWithEvents = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(queryWithEvents);
        when(queryWithEvents.setParameter(anyString(), any())).thenReturn(queryWithEvents);
        when(queryWithEvents.setMaxResults(anyInt())).thenReturn(queryWithEvents);
        when(queryWithEvents.getSingleResult()).thenReturn(5L);
        
        boolean initialCheck = materializer.isMaterialized(aggregateId);
        assertThat(initialCheck).isTrue();
        
        // Paso 2: Usuario borra eventos de PG (DELETE FROM domain_event_entry)
        // Cache aún piensa que está materializado
        assertThat(materializer.isMaterialized(aggregateId)).isTrue(); // Cache hit
        
        // Paso 3: FIX - Sistema detecta inconsistencia y remueve marca
        materializer.removeMaterializedMark(aggregateId);
        
        // Paso 4: Configurar PG como vacío
        TypedQuery<Long> queryEmpty = mock(TypedQuery.class);
        when(entityManager.createQuery(anyString(), eq(Long.class))).thenReturn(queryEmpty);
        when(queryEmpty.setParameter(anyString(), any())).thenReturn(queryEmpty);
        when(queryEmpty.setMaxResults(anyInt())).thenReturn(queryEmpty);
        when(queryEmpty.getSingleResult()).thenReturn(0L);

        // When - Verificar materialización después del fix
        boolean afterFix = materializer.isMaterialized(aggregateId);

        // Then - ✅ FIX: Debe detectar que necesita re-materializar
        assertThat(afterFix).isFalse(); // Detecta que debe re-materializar desde Kafka
    }
}