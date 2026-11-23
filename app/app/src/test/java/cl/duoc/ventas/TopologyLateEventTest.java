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

class TopologyLateEventTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:10Z"); // ventana [00:00,00:01)
    private static final Instant CLOSE = Instant.parse("2026-01-01T00:02:00Z"); // avanza stream time

    @Test
    void eventoDentroDeGraciaSeAgrega() {
        try (var driver = driver()) {
            TestInputTopic<String, String> input = input(driver);
            TestOutputTopic<String, String> output = output(driver);

            // eventTime dentro de la ventana (10 s), gracia 30 s => cierra a 00:01:30.
            input.pipeInput("evt-dentro", TestSupport.saleJson(
                    "evt-dentro", T0.toString(), "Tienda Gracia", 1, 1000), T0);
            // Avanza el stream time mas alla del cierre para forzar Suppress.
            input.pipeInput("evt-trigger", TestSupport.saleJson(
                    "evt-trigger", CLOSE.toString(), "Tienda Trigger", 1, 300), CLOSE);

            JsonNode tienda = ventana(output, "Tienda Gracia");
            assertEquals(1, tienda.path("sales_count").asLong());
            assertEquals(1000, tienda.path("total_clp").asLong());
        }
    }

    @Test
    void eventoFueraDeGraciaNoSeAgregaYNoVaADLQ() {
        try (var driver = driver()) {
            TestInputTopic<String, String> input = input(driver);
            TestOutputTopic<String, String> output = output(driver);

            // Avanzar stream time mas alla del cierre de la ventana [00:00,00:01) + gracia
            // (cierra a 00:01:30). Llevarlo a 00:02:00 deja esa ventana cerrada.
            input.pipeInput("evt-trigger", TestSupport.saleJson(
                    "evt-trigger", CLOSE.toString(), "Tienda Trigger", 1, 300), CLOSE);

            // Evento tardio: eventTime en una ventana ya cerrada => se descarta al llegar.
            input.pipeInput("evt-fuera", TestSupport.saleJson(
                    "evt-fuera", T0.toString(), "Tienda Tardia", 1, 1000), T0);

            List<KeyValue<String, JsonNode>> results = output.readKeyValuesToList().stream()
                    .map(kv -> KeyValue.pair(kv.key, json(kv.value)))
                    .toList();
            assertTrue(results.stream()
                    .noneMatch(kv -> kv.value.path("store").asText().equals("Tienda Tardia")),
                    "El evento tardio no debe agregarse");
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

    private static JsonNode ventana(TestOutputTopic<String, String> output, String store) {
        List<KeyValue<String, JsonNode>> results = output.readKeyValuesToList().stream()
                .map(kv -> KeyValue.pair(kv.key, json(kv.value)))
                .toList();
        return results.stream()
                .map(kv -> kv.value)
                .filter(node -> node.path("store").asText().equals(store))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No se emitio la ventana de " + store));
    }

    private static JsonNode json(String value) {
        try {
            return MAPPER.readTree(value);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
