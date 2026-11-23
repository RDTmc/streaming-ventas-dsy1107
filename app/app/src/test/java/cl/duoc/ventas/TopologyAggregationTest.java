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

class TopologyAggregationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:10Z");
    private static final Instant T1 = Instant.parse("2026-01-01T00:00:20Z");
    private static final Instant CLOSE = Instant.parse("2026-01-01T00:02:00Z");

    @Test
    void agregaPorTiendaEnVentanaDeSesentaSegundos() {
        var topology = StreamsApp.buildTopology(
                TestSupport.RAW, TestSupport.AGG, TestSupport.DLQ,
                TestSupport.WINDOW_SECONDS, TestSupport.GRACE_SECONDS);

        try (var driver = new TopologyTestDriver(topology, TestSupport.streamsProperties())) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    TestSupport.RAW, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    TestSupport.AGG, new StringDeserializer(), new StringDeserializer());

            input.pipeInput("evt-1", TestSupport.saleJson(
                    "evt-1", T0.toString(), "Tienda A", 2, 1000), T0);
            input.pipeInput("evt-2", TestSupport.saleJson(
                    "evt-2", T1.toString(), "Tienda A", 1, 500), T1);
            // Avanza el stream time más allá de la ventana + gracia para que Suppress emita.
            input.pipeInput("evt-3", TestSupport.saleJson(
                    "evt-3", CLOSE.toString(), "Tienda B", 1, 300), CLOSE);

            List<KeyValue<String, JsonNode>> results = output.readKeyValuesToList().stream()
                    .map(kv -> KeyValue.pair(kv.key, json(kv.value)))
                    .toList();

            JsonNode tiendaA = results.stream()
                    .map(kv -> kv.value)
                    .filter(node -> node.path("store").asText().equals("Tienda A"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No se emitió la ventana de Tienda A"));

            assertEquals("2026-01-01T00:00:00Z", tiendaA.path("window_start").asText());
            assertEquals("2026-01-01T00:01:00Z", tiendaA.path("window_end").asText());
            assertEquals(2, tiendaA.path("sales_count").asLong());
            assertEquals(2500, tiendaA.path("total_clp").asLong());
            assertEquals(1250, tiendaA.path("avg_ticket_clp").asLong());
            assertTrue(results.size() >= 1);
        }
    }

    private static JsonNode json(String value) {
        try {
            return MAPPER.readTree(value);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
