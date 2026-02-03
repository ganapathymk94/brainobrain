import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Properties;

public class Sha256AvroStream {

    // Thread-local SHA-256 digest to avoid object churn
    private static final ThreadLocal<MessageDigest> sha256Digest =
        ThreadLocal.withInitial(() -> {
            try {
                return MessageDigest.getInstance("SHA-256");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

    private static String hashPayload(byte[] recordBytes) {
        MessageDigest digest = sha256Digest.get();
        digest.reset();
        byte[] hashBytes = digest.digest(recordBytes);

        StringBuilder sb = new StringBuilder(hashBytes.length * 2);
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sha256-avro-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");

        // Performance tuning
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500); // batch commits
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760); // 10MB cache
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4); // parallelism per instance

        StreamsBuilder builder = new StreamsBuilder();

        GenericAvroSerde avroSerde = new GenericAvroSerde();
        avroSerde.configure(Collections.singletonMap("schema.registry.url", "http://localhost:8081"), false);

        // Example: consume from multiple topics (pattern)
        KStream<String, GenericRecord> inputStream = builder.stream(
                "input-topic-*", // wildcard for multiple topics
                Consumed.with(Serdes.String(), avroSerde)
        );

        KStream<String, String> hashedStream = inputStream.map((key, record) -> {
            try {
                // Dynamically get unique identifier field (config-driven)
                String uniqueFieldName = System.getenv().getOrDefault("UNIQUE_FIELD", "id");
                Object uniqueValue = record.get(uniqueFieldName);

                // Serialize record to bytes
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                writer.write(record, encoder);
                encoder.flush();
                byte[] recordBytes = out.toByteArray();

                // Hash payload
                String hashHex = hashPayload(recordBytes);

                return KeyValue.pair(uniqueValue.toString(), hashHex);

            } catch (Exception e) {
                throw new RuntimeException("Error hashing record", e);
            }
        });

        // Output topic should be compacted
        hashedStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
