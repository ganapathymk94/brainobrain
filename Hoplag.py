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
------;;;--------

import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, from_unixtime, expr, sum as _sum

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

# Parse the _time column as a string to retain the exact timestamp with milliseconds
df = df.withColumn("_time_unix", unix_timestamp(col("_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

# Convert the parsed Unix timestamp back to the original format
df = df.withColumn("_time_parsed", from_unixtime(col("_time_unix"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

# Define a window specification sorted in descending order
windowSpec = Window.partitionBy("order_id").orderBy(col("_time_parsed").desc())

# Add a column for the previous time
df = df.withColumn("prev_time", lag("_time_parsed").over(windowSpec))

# Calculate the time lag in milliseconds using timestamp differences
df = df.withColumn("time_lag", (unix_timestamp(col("_time_parsed")) - unix_timestamp(col("prev_time"))) * 1000 +
                             (expr("cast(date_format(_time_parsed, 'SSS') as int)") - 
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
fig = px.bar(pd_df, x="EventType", y="time_lag", color="order_id", orientation='v',
             title="Time Lag between Events for Each Order ID", 
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

++++++++++++++++

import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp, to_timestamp, expr, sum as _sum

# Create a Spark session
spark = SparkSession.builder.appName("TimeLagCalculation").config("spark.sql.session.timeZone", "UTC").getOrCreate()

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

# Filter to include rows with statusA, statusB, and statusC
df_filtered = df.filter((col("orderStatus") == "statusA") | (col("orderStatus") == "statusB") | (col("orderStatus") == "statusC"))

# Calculate the cumulative time lag for each order_id from statusA to statusC
windowSpecAccum = Window.partitionBy("order_id").orderBy(col("_time").asc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_filtered = df_filtered.withColumn("cumulative_time_lag", expr("sum(time_lag)").over(windowSpecAccum))

# Filter to get rows between statusA and statusC only
df_filtered = df_filtered.filter((col("orderStatus") == "statusA") | (col("orderStatus") == "statusB") | (col("orderStatus") == "statusC"))

# Get the maximum cumulative lag for each order_id between statusA and statusC
df_max_lag = df_filtered.groupBy("order_id").agg(_max("cumulative_time_lag").alias("max_cumulative_time_lag"))

# Join the original DataFrame with the max lag DataFrame to get the event with the maximum cumulative lag
df_filtered = df_filtered.join(df_max_lag, ["order_id"], "left")

# Filter the rows with the maximum cumulative lag
df_max_lag_events = df_filtered.filter(col("cumulative_time_lag") == col("max_cumulative_time_lag"))

# Convert Spark DataFrame to Pandas DataFrame for plotting
pd_df = df_max_lag_events.toPandas()

# Plot the data using Plotly
fig = px.bar(pd_df, x="order_id", y="max_cumulative_time_lag", color="EventType", orientation='v',
             title="Maximum Cumulative Time Lag between Status A and Status C for Each Order ID", 
             labels={"order_id": "Order ID", "max_cumulative_time_lag": "Maximum Cumulative Time Lag (milliseconds)"})

# Customize the hover template
fig.update_traces(hovertemplate='Order ID: %{x}<br>Event Type: %{color}<br>Max Cumulative Time Lag: %{y} ms')

# Show the plot
fig.show()

# Show the DataFrame with the calculated time lags
df_max_lag_events.show(truncate=False)

