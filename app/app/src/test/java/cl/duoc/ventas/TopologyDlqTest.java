package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopologyDlqTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant BASE = Instant.parse("2026-01-01T00:00:10Z");

    @Test
    void losInvalidosVanALaDlqYNoSeAgregan() {
        var topology = StreamsApp.buildTopology(
                TestSupport.RAW, TestSupport.AGG, TestSupport.DLQ,
                TestSupport.WINDOW_SECONDS, TestSupport.GRACE_SECONDS);

        try (var driver = new TopologyTestDriver(topology, TestSupport.streamsProperties())) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    TestSupport.RAW, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> dlq = driver.createOutputTopic(
                    TestSupport.DLQ, new StringDeserializer(), new StringDeserializer());

            String valid = TestSupport.saleJson("evt-ok", BASE.toString(), "Tienda A", 1, 1000);
            String missing = TestSupport.withoutField(
                    TestSupport.saleJson("evt-missing", BASE.toString(), "Tienda A", 1, 1000),
                    "quantity");
            String inconsistent = TestSupport.withInt(
                    TestSupport.saleJson("evt-total", BASE.toString(), "Tienda A", 1, 1000),
                    "totalClp", 999);

            input.pipeInput("evt-ok", valid, BASE);
            input.pipeInput("evt-json", "{", BASE);
            input.pipeInput("evt-missing", missing, BASE);
            input.pipeInput("evt-total", inconsistent, BASE);

            List<JsonNode> envelopes = dlq.readValuesToList().stream()
                    .map(TopologyDlqTest::json)
                    .toList();

            assertEquals(3, envelopes.size());
            assertEquals("JSON_INVALIDO", envelopes.get(0).path("reason").asText());
            assertEquals("CAMPO_OBLIGATORIO:quantity", envelopes.get(1).path("reason").asText());
            assertEquals("TOTAL_INCONSISTENTE", envelopes.get(2).path("reason").asText());

            for (JsonNode envelope : envelopes) {
                assertEquals(TestSupport.RAW, envelope.path("source_topic").asText());
                assertFalse(envelope.path("key").asText().isBlank());
                assertFalse(envelope.path("original").asText().isBlank());
                assertFalse(envelope.path("detected_at").asText().isBlank());
            }
            assertEquals("{", envelopes.get(0).path("original").asText());
        }
    }

    @Test
    void unEventoValidoNoPasaPorLaDlq() {
        var topology = StreamsApp.buildTopology(
                TestSupport.RAW, TestSupport.AGG, TestSupport.DLQ,
                TestSupport.WINDOW_SECONDS, TestSupport.GRACE_SECONDS);

        try (var driver = new TopologyTestDriver(topology, TestSupport.streamsProperties())) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    TestSupport.RAW, new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> dlq = driver.createOutputTopic(
                    TestSupport.DLQ, new StringDeserializer(), new StringDeserializer());

            input.pipeInput("evt-ok", TestSupport.saleJson(
                    "evt-ok", BASE.toString(), "Tienda A", 1, 1000), BASE);

            assertTrue(dlq.isEmpty());
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
