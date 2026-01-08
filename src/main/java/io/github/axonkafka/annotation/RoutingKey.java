package io.github.axonkafka.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Anotación para marcar un método que retorna el routing key 
 * usado para determinar la partición de Kafka.
 * 
 * Ejemplo:
 * <pre>
 * {@code
 * @RoutingKey
 * public String getRoutingKey() {
 *     int tileX = this.x / 100;
 *     int tileY = this.y / 100;
 *     return "tile_" + tileX + "_" + tileY;
 * }
 * }
 * </pre>
 * 
 * Si no se especifica, se usará @TargetAggregateIdentifier como fallback.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RoutingKey {
}