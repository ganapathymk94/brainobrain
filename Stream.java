import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class KafkaStreamProcessor {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Kafka Structured Streaming")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> kafkaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "your_topic")
                .option("startingOffsets", "latest")
                .load();

        // Extract the "value" field (which is a byte array)
        Dataset<Row> parsedDF = kafkaDF.select(
                functions.col("key").cast("string"),
                functions.col("value").cast("string"),
                functions.col("topic"),
                functions.col("partition"),
                functions.col("offset"),
                functions.col("timestamp")
        );

        // Write to console for debugging
        parsedDF.writeStream()
                .outputMode("append")
                .format("console")
                .start()
                .awaitTermination();
    }
}
