package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

/**
 * Usa el campo {@code eventTime} como tiempo de evento para las ventanas.
 * Si el valor no es JSON válido o no trae un eventTime parseable, cae al
 * timestamp del registro: el evento seguirá el camino de validación y terminará
 * en la DLQ si corresponde.
 */
public final class EventTimeExtractor implements TimestampExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof String json) {
            try {
                JsonNode node = MAPPER.readTree(json);
                String eventTime = node.path("eventTime").asText(null);
                if (eventTime != null && !eventTime.isBlank()) {
                    return Instant.parse(eventTime).toEpochMilli();
                }
            } catch (Exception ignored) {
                // JSON inválido o eventTime no parseable: se usa el timestamp del registro.
            }
        }
        return record.timestamp();
    }
}
