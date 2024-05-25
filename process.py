import socket
from pyspark.sql.functions import expr, split, window, count, sum, approx_count_distinct, date_format, col, concat, lit
from pyspark.sql.types import StructType, TimestampType, StringType, IntegerType

host_name = socket.gethostname()

source = spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", f"{host_name}:9092") \
.option("subscribe", "prj-2-input") \
.load()
data = source.select(expr("CAST(value AS STRING)").alias("value"))

split_columns = split(data["value"], ",")

data = data.withColumn("date", split_columns[0].cast(TimestampType())) \
    .withColumn("film_id", split_columns[1]) \
    .withColumn("user_id", split_columns[2]) \
    .withColumn("rate", split_columns[3].cast(IntegerType()))
data = data.drop("value")

movies = spark.read.option("header", True).csv("gs://pojemnik/projekt2/movie_titles.csv")

# Tryb A
win = data.groupBy(
    window("date", "30 days"), data.film_id) \
    .agg(count("rate").alias("rate_count"),
    sum("rate").alias("rate_sum"),
    approx_count_distinct("user_id").alias("user_count")
)
win = win.withColumn("month", date_format(win.window.start, "MM.yyyy"))
win = win.join(movies, movies.ID == win.film_id)
win = win.drop("window", "ID", "Year")

query = win.writeStream \
.format("console") \
.outputMode("complete") \
.start()

# Tryb C
win = data.withWatermark("date", "1 day") \
    .groupBy(
        window("date", "30 days"), data.film_id) \
        .agg(count("rate").alias("rate_count"),
        sum("rate").alias("rate_sum"),
        approx_count_distinct("user_id").alias("user_count")
    )
win = win.withColumn("month", date_format(win.window.start, "MM.yyyy"))
win = win.join(movies, movies.ID == win.film_id)
win = win.drop("window", "ID", "Year")

query = win.writeStream \
.format("console") \
.outputMode("append") \
.start()

# Anomalie
d = 30
l = 2
o = 3
anomalies_window = data.withWatermark("date", f"{d+1} days") \
.groupBy(window("date", f"{d} days", "1 day"), data.film_id) \
.agg(
    count("rate").alias("rate_count"),
    sum("rate").alias("rate_sum")) \
.select(
    col("film_id"),
    col("rate_count"),
    date_format(col("window").start, "dd.MM.yyyy").alias("window_start"),
    date_format(col("window").end, "dd.MM.yyyy").alias("window_end"),
    (col("rate_sum") / col("rate_count")).alias("avg_rate"),
    col("rate_count")
)

anomalies = anomalies_window.where(
    (anomalies_window.rate_count > l) &
    (anomalies_window.avg_rate > o)
).join(movies, movies.ID == anomalies_window.film_id) \
.drop("ID", "Year", "film_id")

anomalies_formatted = anomalies.select(concat(
    col("window_start"),
    lit(","),
    col("window_end"),
    lit(","),
    col("Title"),
    lit(","),
    col("rate_count"),
    lit(","),
    col("avg_rate"),
).alias("value"))

query = anomalies.writeStream \
.format("console") \
.outputMode("complete") \
.start()

anomalies_output = anomalies_formatted.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host_name}:9092") \
    .option("topic", "prj-2-anomalies") \
    .option("checkpointLocation", "./checkpoints/") \
    .outputMode("complete") \
    .start()