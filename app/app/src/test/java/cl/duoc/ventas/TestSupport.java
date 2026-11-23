package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.UUID;

/** Utilidades compartidas por las pruebas de topología. */
final class TestSupport {

    static final String RAW = "ventas-raw";
    static final String AGG = "ventas-agregadas";
    static final String DLQ = "ventas-raw-dlq";
    static final long WINDOW_SECONDS = 60;
    static final long GRACE_SECONDS = 30;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TestSupport() {
    }

    static Properties streamsProperties() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir());
        return props;
    }

    static String saleJson(String eventId, String eventTime, String store,
                           int quantity, int unitPrice) {
        int total = quantity * unitPrice;
        return """
                {"eventId":"%s","eventTime":"%s","store":"%s","storeFormat":"tienda",\
                "city":"Santiago","channel":"presencial","category":"ropa","product":"Polera",\
                "quantity":%d,"unitPriceClp":%d,"totalClp":%d,"customerId":"cliente-0001",\
                "paymentMethod":"debito"}"""
                .formatted(eventId, eventTime, store, quantity, unitPrice, total);
    }

    static String withoutField(String json, String field) {
        ObjectNode node = node(json);
        node.remove(field);
        return write(node);
    }

    static String withInt(String json, String field, int value) {
        ObjectNode node = node(json);
        node.put(field, value);
        return write(node);
    }

    static String withText(String json, String field, String value) {
        ObjectNode node = node(json);
        node.put(field, value);
        return write(node);
    }

    private static ObjectNode node(String json) {
        try {
            return (ObjectNode) MAPPER.readTree(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String write(ObjectNode node) {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String stateDir() {
        try {
            return Files.createTempDirectory("kafka-streams-test").toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
