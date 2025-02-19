import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp, expr, max as _max

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

# Define a window specification ordered by _time in ascending order
windowSpec = Window.partitionBy("order_id").orderBy(col("_time").asc())

# Add a column for the previous time
df = df.withColumn("prev_time", lag("_time").over(windowSpec))

# Calculate the time lag in milliseconds using timestamp differences
df = df.withColumn("time_lag", (unix_timestamp(col("_time")) - unix_timestamp(col("prev_time"))) * 1000 +
                             (expr("cast(date_format(_time, 'SSS') as int)") - 
                              expr("cast(date_format(prev_time, 'SSS') as int)")))

# Replace null values in the time_lag column with 0 (for the first event of each order_id)
df = df.withColumn("time_lag", col("time_lag").cast("long"))
df = df.fillna(0, subset=["time_lag"])

# Get the maximum lag for each order_id
df_max_lag = df.groupBy("order_id").agg(_max("time_lag").alias("max_time_lag"))

# Join the original DataFrame with the max lag DataFrame to get the event with the maximum lag
df = df.join(df_max_lag, ["order_id"])

# Filter the rows with the maximum lag
df_max_lag_events = df.filter(col("time_lag") == col("max_time_lag"))

# Convert Spark DataFrame to Pandas DataFrame for plotting
pd_df = df_max_lag_events.toPandas()

# Plot the data using Plotly
fig = px.bar(pd_df, x="order_id", y="max_time_lag", color="EventType", orientation='v',
             title="Maximum Time Lag for Each Order ID", 
             labels={"order_id": "Order ID", "max_time_lag": "Maximum Time Lag (milliseconds)"})

# Customize the hover template
fig.update_traces(hovertemplate='Order ID: %{x}<br>Event Type: %{color}<br>Max Time Lag: %{y} ms')

# Show the plot
fig.show()

# Show the DataFrame with the calculated time lags
df_max_lag_events.show(truncate=False)
