package com.example.reconciliation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Properties;
import java.util.regex.Pattern;

@SpringBootApplication
public class ReconciliationServiceApp {

    public static void main(String[] args) {
        SpringApplication.run(ReconciliationServiceApp.class, args);
    }

    @Bean
    public KafkaStreams kafkaStreams() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reconciliation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE); // Java 8 safe

        StreamsBuilder builder = new StreamsBuilder();

        // Subscribe to all topics (wildcard)
        KStream<String, String> inputStream = builder.stream(Pattern.compile(".*"));

        final ObjectMapper mapper = new ObjectMapper();

        // Enrich with reconciliation metadata
        KStream<String, String> enrichedStream = inputStream.transformValues(new ValueTransformerWithKeySupplier<String, String, String>() {
            @Override
            public ValueTransformerWithKey<String, String, String> get() {
                return new ValueTransformerWithKey<String, String, String>() {
                    private ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public String transform(String readOnlyKey, String value) {
                        try {
                            JsonNode json = mapper.readTree(value);

                            String traceId = json.hasNonNull("traceId") ? json.get("traceId").asText() : "MISSING_TRACEID";
                            String payloadHash = DigestUtils.sha256Hex(value);
                            String sourceTopic = context.topic();

                            long timestamp = System.currentTimeMillis();
                            String status = "SUCCESS";
                            if ("MISSING_TRACEID".equals(traceId)) {
                                status = "FAILED";
                            }

                            return String.format(
                                "{\"traceId\":\"%s\",\"payloadHash\":\"%s\",\"sourceTopic\":\"%s\",\"timestamp\":%d,\"status\":\"%s\"}",
                                traceId, payloadHash, sourceTopic, timestamp, status
                            );
                        } catch (Exception e) {
                            return String.format(
                                "{\"traceId\":\"error\",\"payloadHash\":\"\",\"sourceTopic\":\"%s\",\"timestamp\":%d,\"status\":\"FAILED\"}",
                                context.topic(), System.currentTimeMillis()
                            );
                        }
                    }

                    @Override
                    public void close() {
                        // no-op
                    }
                };
            }
        });

        // Repartition into fixed partition count (e.g., 24)
        KStream<String, String> repartitioned = enrichedStream
            .selectKey(new KeyValueMapper<String, String, String>() {
                @Override
                public String apply(String key, String value) {
                    try {
                        JsonNode json = mapper.readTree(value);
                        return json.get("traceId").asText();
                    } catch (Exception e) {
                        return "error";
                    }
                }
            })
            .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withNumberOfPartitions(24));

        // Materialize state store
        KTable<String, String> reconciliationTable = repartitioned
            .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reconciliation-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()));

        // Forward to compacted audit topic
        reconciliationTable.toStream().to("reconciliation.audit",
            Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));

        return streams;
    }
}

@RestController
@RequestMapping("/api/reconcile")
class ReconciliationController {

    private final KafkaStreams kafkaStreams;

    public ReconciliationController(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @GetMapping
    public ResponseEntity<String> reconcile(@RequestParam String traceId,
                                            @RequestParam(required = false) String sourceTopic) {
        ReadOnlyKeyValueStore<String, String> store =
            kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                "reconciliation-store", QueryableStoreTypes.keyValueStore()));

        String result = store.get(traceId);

        if (result != null) {
            if (sourceTopic == null || result.contains("\"sourceTopic\":\"" + sourceTopic + "\"")) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body("{\"traceId\":\"" + traceId + "\",\"status\":\"NOT_FOUND_IN_TOPIC\"}");
            }
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("{\"traceId\":\"" + traceId + "\",\"status\":\"NOT_FOUND\"}");
        }
    }
}
