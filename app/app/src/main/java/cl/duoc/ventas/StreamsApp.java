package cl.duoc.ventas;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public final class StreamsApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsApp.class);

    private static final String STATE_STORE = "ventas-por-tienda";

    public static void main(String[] args) {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String rawTopic = env("RAW_TOPIC", "ventas-raw");
        String aggTopic = env("AGG_TOPIC", "ventas-agregadas");
        String dlqTopic = env("DLQ_TOPIC", "ventas-raw-dlq");
        String applicationId = env("GROUP_ID", "ventas-aggregator");
        long windowSeconds = Long.parseLong(env("WINDOW_SECONDS", "60"));
        long graceSeconds = Long.parseLong(env("WINDOW_GRACE_SECONDS", "30"));

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STREAMS_STATE_DIR", "/tmp/kafka-streams"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var streams = new KafkaStreams(
                buildTopology(rawTopic, aggTopic, dlqTopic, windowSeconds, graceSeconds),
                props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        LOG.info("Kafka Streams app={} raw={} agg={} dlq={} ventana={}s gracia={}s",
                applicationId, rawTopic, aggTopic, dlqTopic, windowSeconds, graceSeconds);
        streams.start();
    }

    public static org.apache.kafka.streams.Topology buildTopology(
            String rawTopic, String aggTopic, String dlqTopic, long windowSeconds, long graceSeconds) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> raw = builder.stream(
                rawTopic,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new EventTimeExtractor()));

        KStream<String, ValidationResult> checked = raw
                .mapValues(SaleValidator::validate, Named.as("validar-evento"));

        checked
                .filter((key, result) -> !result.valid(), Named.as("eventos-invalidos"))
                .mapValues((key, result) -> DlqEnvelope.toJson(key, rawTopic, result, result.original()),
                        Named.as("sobre-dlq"))
                .peek((key, envelope) -> LOG.warn("evento enviado a DLQ topic={} key={} {}", dlqTopic, key, envelope))
                .to(dlqTopic, Produced.with(Serdes.String(), Serdes.String()));

        TimeWindows windows = TimeWindows.ofSizeAndGrace(
                Duration.ofSeconds(windowSeconds),
                Duration.ofSeconds(graceSeconds));

        KTable<Windowed<String>, TicketAggregate> aggregate = checked
                .filter((key, result) -> result.valid(), Named.as("eventos-validos"))
                .mapValues(ValidationResult::sale, Named.as("dto-venta"))
                .selectKey((key, sale) -> sale.store(), Named.as("rekey-por-tienda"))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Sale.class)))
                .windowedBy(windows)
                .aggregate(
                        TicketAggregate::new,
                        (store, sale, totals) -> totals.add(sale),
                        Materialized.<String, TicketAggregate, WindowStore<Bytes, byte[]>>as(STATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TicketAggregate.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        aggregate.toStream()
                .map((windowedKey, totals) -> KeyValue.pair(
                        windowedKey.key(), resultJson(windowedKey, totals)))
                .to(aggTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static String resultJson(Windowed<String> windowedKey, TicketAggregate totals) {
        long average = totals.count == 0 ? 0 : Math.round((double) totals.totalClp / totals.count);
        var result = new WindowResult(
                windowedKey.window().startTime().toString(),
                windowedKey.window().endTime().toString(),
                windowedKey.key(),
                totals.count,
                totals.totalClp,
                average);
        try {
            return JsonSerde.mapper().writeValueAsString(result);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo serializar el agregado", e);
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
