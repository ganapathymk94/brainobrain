from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp, expr, sum as _sum
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

# Calculate the time lag in milliseconds using timestamp differences
df = df.withColumn("time_lag", (unix_timestamp(col("_time")) - unix_timestamp(col("prev_time"))) * 1000 +
                             (expr("cast(date_format(_time, 'SSS') as int)") - 
                              expr("cast(date_format(prev_time, 'SSS') as int)")))

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


import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp, expr, sum as _sum

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

# Calculate the time lag in milliseconds using timestamp differences
df = df.withColumn("time_lag", (unix_timestamp(col("_time")) - unix_timestamp(col("prev_time"))) * 1000 +
                             (expr("cast(date_format(_time, 'SSS') as int)") - 
                              expr("cast(date_format(prev_time, 'SSS') as int)")))

# Replace null values in the time_lag column with 0 (for the first event of each order_id)
df = df.withColumn("time_lag", col("time_lag").cast("long"))
df = df.withColumn("time_lag", col("time_lag").na.fill(0))

# Convert Spark DataFrame to Pandas DataFrame for plotting
pd_df = df.toPandas()

# Plot the data using Plotly
fig = px.bar(pd_df, x="EventType", y="time_lag", color="order_id", title="Time Lag between Events for Each Order ID", 
             labels={"EventType": "Event Type", "time_lag": "Time Lag (milliseconds)"})

# Customize the hover template
fig.update_traces(hovertemplate='Order ID: %{color}<br>Event Type: %{x}<br>Time Lag: %{y} ms')

# Show the plot
fig.show()

----------


import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp, expr, sum as _sum

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

# Calculate the time lag in milliseconds using timestamp differences
df = df.withColumn("time_lag", (unix_timestamp(col("_time")) - unix_timestamp(col("prev_time"))) * 1000 +
                             (expr("cast(date_format(_time, 'SSS') as int)") - 
                              expr("cast(date_format(prev_time, 'SSS') as int)")))

# Replace null values in the time_lag column with 0 (for the first event of each order_id)
df = df.withColumn("time_lag", col("time_lag").cast("long"))
df = df.withColumn("time_lag", col("time_lag").na.fill(0))

# Filter to include rows with statusA and statusC
df_filtered = df.filter((col("orderStatus") == "statusA") | (col("orderStatus") == "statusC"))

# Group by order_id and calculate the total time lag between statusA and statusC
df_grouped = df_filtered.groupBy("order_id").agg(_sum("time_lag").alias("total_time_lag"))

# Find the order_id with the maximum total time lag
max_time_lag_df = df_grouped.orderBy(col("total_time_lag").desc()).limit(1)
max_order_id = max_time_lag_df.collect()[0]["order_id"]

# Convert Spark DataFrame to Pandas DataFrame for plotting
pd_df = df.toPandas()

# Plot the data using Plotly
fig = px.bar(pd_df, x="EventType", y="time_lag", color="order_id", title="Time Lag between Events for Each Order ID", 
             labels={"EventType": "Event Type", "time_lag": "Time Lag (milliseconds)"})

# Customize the hover template
fig.update_traces(hovertemplate='Order ID: %{color}<br>Event Type: %{x}<br>Time Lag: %{y} ms')

# Highlight the order with the maximum lag
fig.add_vline(x=pd_df[pd_df["order_id"] == max_order_id]["EventType"].values[0], line_width=3, line_dash="dash", line_color="red")

# Add annotation for the order with the maximum lag
fig.add_annotation(x=pd_df[pd_df["order_id"] == max_order_id]["EventType"].values[0],
                   y=pd_df[pd_df["order_id"] == max_order_id]["time_lag"].max(),
                   text=f"Max Lag Order: {max_order_id}",
                   showarrow=True,
                   arrowhead=1)

# Show the plot
fig.show()

