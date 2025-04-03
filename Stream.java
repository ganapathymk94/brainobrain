import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;

public class JsonExtractor {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Kafka JSON Extractor")
                .master("local[*]")
                .getOrCreate();

        // Define JSON Schema
        StructType schema = new StructType()
                .add("id", DataTypes.StringType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);

        // Read Kafka Stream
        Dataset<Row> kafkaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "your_topic")
                .option("startingOffsets", "latest")
                .load();

        // Convert Kafka Value (Byte Array) to String
        Dataset<Row> jsonDF = kafkaDF.selectExpr("CAST(value AS STRING) as json_string");

        // Parse JSON
        Dataset<Row> parsedDF = jsonDF.select(
                functions.from_json(functions.col("json_string"), schema).alias("data")
        );

        // Extract Fields
        Dataset<Row> finalDF = parsedDF.select("data.*");

        // Write to Console for Debugging
        finalDF.writeStream()
                .outputMode("append")
                .format("console")
                .start()
                .awaitTermination();
    }
}
