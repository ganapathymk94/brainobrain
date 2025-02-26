import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class TimeLagCalculation {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TimeLagCalculation")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        // Sample data for demonstration
        Dataset<Row> data = spark.createDataFrame(List.of(
                RowFactory.create("order1", "Event1", "statusA", "2023-02-19T10:00:00.123+0000"),
                RowFactory.create("order1", "Event2", "statusB", "2023-02-19T10:01:00.234+0000"),
                RowFactory.create("order1", "Event3", "statusC", "2023-02-19T10:02:00.345+0000"),
                RowFactory.create("order2", "Event1", "statusA", "2023-02-19T11:00:00.123+0000"),
                RowFactory.create("order2", "Event2", "statusB", "2023-02-19T11:01:00.234+0000")
        ), new StructType(new StructField[]{
                new StructField("order_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("EventType", DataTypes.StringType, false, Metadata.empty()),
                new StructField("orderStatus", DataTypes.StringType, false, Metadata.empty()),
                new StructField("_time", DataTypes.StringType, false, Metadata.empty())
        }));

        // Cast the _time column to a timestamp
        data = data.withColumn("_time", to_timestamp(col("_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"));

        // Define a window specification ordered by _time in ascending order
        WindowSpec windowSpec = Window.partitionBy("order_id").orderBy(col("_time").asc());

        // Add a column for the previous time
        data = data.withColumn("prev_time", lag("_time", 1).over(windowSpec));

        // Calculate the time lag in milliseconds using timestamp differences
        data = data.withColumn("time_lag", 
                (unix_timestamp(col("_time")).minus(unix_timestamp(col("prev_time")))).multiply(1000)
                .plus(expr("cast(date_format(_time, 'SSS') as int)").minus(expr("cast(date_format(prev_time, 'SSS') as int)"))));

        // Replace null values in the time_lag column with 0 (for the first event of each order_id)
        data = data.withColumn("time_lag", col("time_lag").cast("long"));
        data = data.na().fill(0, new String[]{"time_lag"});

        // Filter to include rows with statusA, statusB, and statusC
        data = data.filter(col("orderStatus").equalTo("statusA")
                .or(col("orderStatus").equalTo("statusB"))
                .or(col("orderStatus").equalTo("statusC")));

        // Calculate the cumulative time lag for each order_id from statusA to statusC
        WindowSpec windowSpecAccum = Window.partitionBy("order_id").orderBy(col("_time").asc())
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        data = data.withColumn("cumulative_time_lag", sum(col("time_lag")).over(windowSpecAccum));

        // Get the maximum cumulative lag for each order_id between statusA and statusC
        Dataset<Row> dfMaxLag = data.groupBy("order_id").agg(max(col("cumulative_time_lag")).alias("max_cumulative_time_lag"));

        // Join the original DataFrame with the max lag DataFrame to get the event with the maximum cumulative lag
        data = data.join(dfMaxLag, "order_id");

        // Filter the rows with the maximum cumulative lag
        Dataset<Row> dfMaxLagEvents = data.filter(col("cumulative_time_lag").equalTo(col("max_cumulative_time_lag")));

        // Show the DataFrame with the calculated time lags
        dfMaxLagEvents.show(false);
    }
}
