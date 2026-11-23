package cl.duoc.ventas;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;

/**
 * Sobre que se publica en la DLQ. Conserva el valor original completo para poder
 * reprocesar o auditar el evento rechazado.
 */
public record DlqEnvelope(
        @JsonProperty("reason") String reason,
        @JsonProperty("source_topic") String sourceTopic,
        @JsonProperty("key") String key,
        @JsonProperty("event_id") String eventId,
        @JsonProperty("original") String original,
        @JsonProperty("detected_at") String detectedAt) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String toJson(String key, String sourceTopic, ValidationResult result, String original) {
        var envelope = new DlqEnvelope(
                result.reason(),
                sourceTopic,
                key,
                result.eventId(),
                original,
                Instant.now().toString());
        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo serializar el sobre de DLQ", e);
        }
    }
}
