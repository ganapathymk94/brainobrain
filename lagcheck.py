from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder.appName("TimeLagCalculation").getOrCreate()

# Sample data for demonstration
data = [
    ("order1", "Event1", "statusA", "2023-02-19 10:00:00.123"),
    ("order1", "Event2", "statusB", "2023-02-19 10:01:00.234"),
    ("order1", "Event3", "statusC", "2023-02-19 10:02:00.345"),
    ("order2", "Event1", "statusA", "2023-02-19 11:00:00.123"),
    ("order2", "Event2", "statusB", "2023-02-19 11:01:00.234"),
]

# Create a DataFrame
columns = ["order_id", "EventType", "orderStatus", "_time"]
df = spark.createDataFrame(data, columns)

# Cast the _time column to a timestamp
df = df.withColumn("_time", to_timestamp(col("_time"), "yyyy-MM-dd HH:mm:ss.SSS"))

# Define a window specification
windowSpec = Window.partitionBy("order_id").orderBy("_time")

# Add a column for the previous time
df = df.withColumn("prev_time", lag("_time").over(windowSpec))

# Convert time columns to unix timestamps and calculate the time lag in milliseconds
df = df.withColumn("time_lag", (unix_timestamp(col("_time")) - unix_timestamp(col("prev_time"))) * 1000)

# Replace null values in the time_lag column with 0 (for the first event of each order_id)
df = df.withColumn("time_lag", col("time_lag").cast("long"))
df = df.withColumn("time_lag", col("time_lag").na.fill(0))

# Convert Spark DataFrame to Pandas DataFrame
pd_df = df.toPandas()

# Plot the data using Seaborn
sns.set(style="whitegrid")
plt.figure(figsize=(12, 6))

# Create a bar plot
sns.barplot(x="EventType", y="time_lag", hue="order_id", data=pd_df)

# Customize the plot
plt.title("Time Lag between Events for Each Order ID")
plt.xlabel("Event Type")
plt.ylabel("Time Lag (milliseconds)")
plt.legend(title="Order ID")

# Display the plot
plt.show()
------------------------------------------------------


from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder.appName("TimeLagCalculation").getOrCreate()

# Sample data for demonstration
data = [
    ("order1", "Event1", "statusA", "2023-02-19T10:00:00.123+0000"),
    ("order1", "Event2", "statusB", "2023-02-19T10:01:00.234+0000"),
    ("order1", "Event3", "statusC", "2023-02-19T10:02:00.345+0000"),
    ("order2", "Event1", "statusA", "2023-02-19T11:00:00.123+0000"),
    ("order2", "Event2", "statusB", "2023-02-19T11:01:00.234+0000"),
]

# Create a DataFrame
columns = ["order_id", "EventType", "orderStatus", "_time"]
df = spark.createDataFrame(data, columns)

# Cast the _time column to a timestamp
df = df.withColumn("_time", to_timestamp(col("_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

# Define a window specification
windowSpec = Window.partitionBy("order_id").orderBy("_time")

# Add a column for the previous time
df = df.withColumn("prev_time", lag("_time").over(windowSpec))

# Convert time columns to unix timestamps and calculate the time lag in milliseconds
df = df.withColumn("time_lag", (unix_timestamp(col("_time")) - unix_timestamp(col("prev_time"))) * 1000)

# Replace null values in the time_lag column with 0 (for the first event of each order_id)
df = df.withColumn("time_lag", col("time_lag").cast("long"))
df = df.withColumn("time_lag", col("time_lag").na.fill(0))

# Convert Spark DataFrame to Pandas DataFrame
pd_df = df.toPandas()

# Plot the data using Seaborn
sns.set(style="whitegrid")
plt.figure(figsize=(12, 6))

# Create a bar plot
sns.barplot(x="EventType", y="time_lag", hue="order_id", data=pd_df)

# Customize the plot
plt.title("Time Lag between Events for Each Order ID")
plt.xlabel("Event Type")
plt.ylabel("Time Lag (milliseconds)")
plt.legend(title="Order ID")

# Display the plot
plt.show()
--------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp, sum as _sum, max as _max
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder.appName("TimeLagCalculation").getOrCreate()

# Sample data for demonstration
data = [
    ("order1", "Event1", "statusA", "2023-02-19T10:00:00.123+0000"),
    ("order1", "Event2", "statusB", "2023-02-19T10:01:00.234+0000"),
    ("order1", "Event3", "statusC", "2023-02-19T10:02:00.345+0000"),
    ("order2", "Event1", "statusA", "2023-02-19T11:00:00.123+0000"),
    ("order2", "Event2", "statusB", "2023-02-19T11:01:00.234+0000"),
]

# Create a DataFrame
columns = ["order_id", "EventType", "orderStatus", "_time"]
df = spark.createDataFrame(data, columns)

# Cast the _time column to a timestamp
df = df.withColumn("_time", to_timestamp(col("_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

# Define a window specification
windowSpec = Window.partitionBy("order_id").orderBy("_time")

# Add a column for the previous time
df = df.withColumn("prev_time", lag("_time").over(windowSpec))

# Convert time columns to unix timestamps and calculate the time lag in milliseconds
df = df.withColumn("time_lag", (unix_timestamp(col("_time")) - unix_timestamp(col("prev_time"))) * 1000)

# Replace null values in the time_lag column with 0 (for the first event of each order_id)
df = df.withColumn("time_lag", col("time_lag").cast("long"))
df = df.withColumn("time_lag", col("time_lag").na.fill(0))

# Filter to only include rows with statusA and statusC
df_filtered = df.filter((col("orderStatus") == "statusA") | (col("orderStatus") == "statusC"))

# Group by order_id and calculate the total time lag between statusA and statusC
df_grouped = df_filtered.groupBy("order_id").agg(_sum("time_lag").alias("total_time_lag"))

# Find the order_id with the maximum total time lag
max_time_lag_df = df_grouped.orderBy(col("total_time_lag").desc()).limit(1)

# Show the order_id with the maximum time lag
max_time_lag_df.show()

# Convert Spark DataFrame to Pandas DataFrame for plotting
pd_df = df.toPandas()

# Plot the data using Seaborn
sns.set(style="whitegrid")
plt.figure(figsize=(12, 6))

# Create a bar plot
sns.barplot(x="EventType", y="time_lag", hue="order_id", data=pd_df)

# Customize the plot
plt.title("Time Lag between Events for Each Order ID")
plt.xlabel("Event Type")
plt.ylabel("Time Lag (milliseconds)")
plt.legend(title="Order ID")

# Display the plot
plt.show()
---------+---++--------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp, sum as _sum, expr
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder.appName("TimeLagCalculation").getOrCreate()

# Sample data for demonstration
data = [
    ("order1", "Event1", "statusA", "2023-02-19T10:00:00.123+0000"),
    ("order1", "Event2", "statusB", "2023-02-19T10:01:00.234+0000"),
    ("order1", "Event3", "statusC", "2023-02-19T10:02:00.345+0000"),
    ("order2", "Event1", "statusA", "2023-02-19T11:00:00.123+0000"),
    ("order2", "Event2", "statusB", "2023-02-19T11:01:00.234+0000"),
]

# Create a DataFrame
columns = ["order_id", "EventType", "orderStatus", "_time"]
df = spark.createDataFrame(data, columns)

# Cast the _time column to a timestamp
df = df.withColumn("_time", to_timestamp(col("_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

# Define a window specification
windowSpec = Window.partitionBy("order_id").orderBy("_time")

# Add a column for the previous time
df = df.withColumn("prev_time", lag("_time").over(windowSpec))

# Calculate the time lag in milliseconds
df = df.withColumn("time_lag", (unix_timestamp(col("_time")) * 1000 + expr("microsecond(_time) / 1000")) - 
                             (unix_timestamp(col("prev_time")) * 1000 + expr("microsecond(prev_time) / 1000")))

# Replace null values in the time_lag column with 0 (for the first event of each order_id)
df = df.withColumn("time_lag", col("time_lag").cast("long"))
df = df.withColumn("time_lag", col("time_lag").na.fill(0))

# Filter to include rows with statusA and statusC
df_filtered = df.filter((col("orderStatus") == "statusA") | (col("orderStatus") == "statusC"))

# Group by order_id and calculate the total time lag between statusA and statusC
df_grouped = df_filtered.groupBy("order_id").agg(_sum("time_lag").alias("total_time_lag"))

# Find the order_id with the maximum total time lag
max_time_lag_df = df_grouped.orderBy(col("total_time_lag").desc()).limit(1)

# Show the order_id with the maximum time lag
max_time_lag_df.show()

# Convert Spark DataFrame to Pandas DataFrame for plotting
pd_df = df.toPandas()

# Plot the data using Seaborn
sns.set(style="whitegrid")
plt.figure(figsize=(12, 6))

# Create a bar plot
sns.barplot(x="EventType", y="time_lag", hue="order_id", data=pd_df)

# Customize the plot
plt.title("Time Lag between Events for Each Order ID")
plt.xlabel("Event Type")
plt.ylabel("Time Lag (milliseconds)")
plt.legend(title="Order ID")

# Display the plot
plt.show()

