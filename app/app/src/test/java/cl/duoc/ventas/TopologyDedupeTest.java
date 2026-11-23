package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopologyDedupeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:10Z");
    private static final Instant T1 = Instant.parse("2026-01-01T00:00:20Z");
    private static final Instant CLOSE = Instant.parse("2026-01-01T00:02:00Z");

    @Test
    void elMismoEventoNoSeCuentaDosVeces() {
        try (var driver = driver()) {
            TestInputTopic<String, String> input = input(driver);
            TestOutputTopic<String, String> output = output(driver);

            String repetido = TestSupport.saleJson("evt-1", T0.toString(), "Tienda A", 2, 1000);
            input.pipeInput("evt-1", repetido, T0);
            input.pipeInput("evt-1", repetido, T1);
            input.pipeInput("evt-2", TestSupport.saleJson(
                    "evt-2", T1.toString(), "Tienda A", 1, 500), T1);
            input.pipeInput("evt-3", TestSupport.saleJson(
                    "evt-3", CLOSE.toString(), "Tienda B", 1, 300), CLOSE);

            JsonNode tiendaA = ventanaDe(output, "Tienda A");
            assertEquals(2, tiendaA.path("sales_count").asLong());
            assertEquals(2500, tiendaA.path("total_clp").asLong());
        }
    }

    @Test
    void siElPayloadDifiereSeConservaElPrimero() {
        try (var driver = driver()) {
            TestInputTopic<String, String> input = input(driver);
            TestOutputTopic<String, String> output = output(driver);

            String primero = TestSupport.saleJson("evt-1", T0.toString(), "Tienda A", 2, 1000);
            String segundo = TestSupport.withInt(primero, "totalClp", 9999);
            input.pipeInput("evt-1", primero, T0);
            input.pipeInput("evt-1", segundo, T1);
            input.pipeInput("evt-2", TestSupport.saleJson(
                    "evt-2", CLOSE.toString(), "Tienda B", 1, 300), CLOSE);

            JsonNode tiendaA = ventanaDe(output, "Tienda A");
            assertEquals(1, tiendaA.path("sales_count").asLong());
            assertEquals(2000, tiendaA.path("total_clp").asLong());
        }
    }

    private static TopologyTestDriver driver() {
        return new TopologyTestDriver(
                StreamsApp.buildTopology(
                        TestSupport.RAW, TestSupport.AGG, TestSupport.DLQ,
                        TestSupport.WINDOW_SECONDS, TestSupport.GRACE_SECONDS),
                TestSupport.streamsProperties());
    }

    private static TestInputTopic<String, String> input(TopologyTestDriver driver) {
        return driver.createInputTopic(
                TestSupport.RAW, new StringSerializer(), new StringSerializer());
    }

    private static TestOutputTopic<String, String> output(TopologyTestDriver driver) {
        return driver.createOutputTopic(
                TestSupport.AGG, new StringDeserializer(), new StringDeserializer());
    }

    private static JsonNode ventanaDe(TestOutputTopic<String, String> output, String store) {
        List<KeyValue<String, JsonNode>> results = output.readKeyValuesToList().stream()
                .map(kv -> KeyValue.pair(kv.key, json(kv.value)))
                .toList();
        assertTrue(results.size() >= 1, "No se emitió ningún agregado");
        return results.stream()
                .map(kv -> kv.value)
                .filter(node -> node.path("store").asText().equals(store))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No se emitió la ventana de " + store));
    }

    private static JsonNode json(String value) {
        try {
            return MAPPER.readTree(value);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
