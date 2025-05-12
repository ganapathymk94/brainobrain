import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;

@Component
public class ApacheKafkaStreamsService {

    private KafkaStreams streams;

    @PostConstruct
    public void startKafkaStreams() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "apache-kafka-streams-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: Read Kafka Topic
        KStream<String, Long> stream = builder.stream("your-kafka-topic",
                Consumed.with(Serdes.String(), Serdes.Long()));

        // Step 2: Group by Topic-Partition
        KGroupedStream<String, Long> groupedByPartition = stream.groupByKey();

        // Step 3: Compute Latest Offset per Partition
        KTable<String, Long> latestPartitionOffsets = groupedByPartition.aggregate(
                () -> 0L,
                (key, newOffset, currentTotal) -> Math.max(newOffset, currentTotal),
                Materialized.with(Serdes.String(), Serdes.Long())
        );

        // Step 4: Convert Back to Topic-Level Aggregation (Summing Partition Offsets)
        KTable<String, Long> totalMessageCountPerTopic = latestPartitionOffsets
                .toStream()
                .groupBy((topicPartitionKey, offset) -> extractTopicName(topicPartitionKey))
                .reduce((offset1, offset2) -> offset1 + offset2, Materialized.with(Serdes.String(), Serdes.Long()));

        // Step 5: Save to Database
        totalMessageCountPerTopic.toStream().foreach((topic, totalCount) -> saveToDatabase(topic, totalCount));

        streams = new KafkaStreams(builder.build(), config);
        streams.start();  // Starts Kafka Streams when the app starts
    }

    @PreDestroy
    public void stopKafkaStreams() {
        if (streams != null) {
            streams.close();
        }
    }

    private String extractTopicName(String topicPartitionKey) {
        return topicPartitionKey.split("-")[0]; // Extract topic name from "topic-partition"
    }

    private void saveToDatabase(String topic, Long totalCount) {
        System.out.println("Saving to DB → Topic: " + topic + ", Messages: " + totalCount);
        // Implement actual DB insert logic
    }
}


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Properties;

@Service
public class KafkaStreamsService {

    private KafkaStreams streams;

    @PostConstruct
    public void startKafkaStreams() {
        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: Read Kafka Topic
        KStream<String, Long> stream = builder.stream("your-kafka-topic",
                Consumed.with(Serdes.String(), Serdes.Long()));

        // Step 2: Group by Topic-Partition
        KGroupedStream<String, Long> groupedByPartition = stream.groupByKey();

        // Step 3: Compute Latest Offset per Partition
        KTable<String, Long> latestPartitionOffsets = groupedByPartition.aggregate(
                () -> 0L,
                (key, newOffset, currentTotal) -> Math.max(newOffset, currentTotal),
                Materialized.with(Serdes.String(), Serdes.Long())
        );

        // Step 4: Convert Back to Topic-Level Aggregation (Summing Partition Offsets)
        KTable<String, Long> totalMessageCountPerTopic = latestPartitionOffsets
                .toStream()
                .groupBy((topicPartitionKey, offset) -> extractTopicName(topicPartitionKey)) // Group by topic
                .reduce((offset1, offset2) -> offset1 + offset2, Materialized.with(Serdes.String(), Serdes.Long())); // Sum partitions

        // Step 5: Save to Database
        totalMessageCountPerTopic.toStream().foreach((topic, totalCount) -> {
            saveToDatabase(topic, totalCount);
        });

        // Start Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    private String extractTopicName(String topicPartitionKey) {
        return topicPartitionKey.split("-")[0]; // Extract topic name from "topic-partition"
    }

    private void saveToDatabase(String topic, Long totalCount) {
        System.out.println("Saving to DB → Topic: " + topic + ", Messages: " + totalCount);
        // Implement actual DB insert logic
    }

    public void stopKafkaStreams() {
        if (streams != null) {
            streams.close();
        }
    }
}

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;

@Component
public class ApacheKafkaStreamsService {

    private KafkaStreams streams;

    @PostConstruct
    public void startKafkaStreams() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "apache-kafka-streams-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName()); // Avro Serde
        config.put("schema.registry.url", "http://localhost:8081"); // Update with your Schema Registry URL

        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: Read Kafka Topic with Avro Data
        KStream<String, GenericRecord> stream = builder.stream("your-kafka-topic",
                Consumed.with(Serdes.String(), new GenericAvroSerde()));

        // Step 2: Extract Message Count from Avro Record
        KStream<String, Long> processedStream = stream.mapValues(record -> 
                (Long) record.get("messagesCount") // Extract the "messagesCount" field
        );

        // Step 3: Group by Topic
        KGroupedStream<String, Long> groupedByTopic = processedStream.groupByKey();

        // Step 4: Compute Latest Offset per Partition
        KTable<String, Long> latestPartitionOffsets = groupedByTopic.aggregate(
                () -> 0L,
                (key, newOffset, currentTotal) -> Math.max(newOffset, currentTotal),
                Materialized.with(Serdes.String(), Serdes.Long())
        );

        // Step 5: Convert Back to Topic-Level Aggregation (Summing Partition Offsets)
        KTable<String, Long> totalMessageCountPerTopic = latestPartitionOffsets
                .toStream()
                .groupBy((topicPartitionKey, offset) -> extractTopicName(topicPartitionKey))
                .reduce((offset1, offset2) -> offset1 + offset2, Materialized.with(Serdes.String(), Serdes.Long()));

        // Step 6: Save to Database
        totalMessageCountPerTopic.toStream().foreach((topic, totalCount) -> saveToDatabase(topic, totalCount));

        streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    @PreDestroy
    public void stopKafkaStreams() {
        if (streams != null) {
            streams.close();
        }
    }

    private String extractTopicName(String topicPartitionKey) {
        return topicPartitionKey.split("-")[0]; // Extract topic name from "topic-partition"
    }

    private void saveToDatabase(String topic, Long totalCount) {
        System.out.println("Saving to DB → Topic: " + topic + ", Messages: " + totalCount);
        // Implement actual DB insert logic
    }
}
