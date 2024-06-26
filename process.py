import socket
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, split, window, count, sum, approx_count_distinct, date_format, col, concat, lit, to_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame

def process_realtime_data(data: DataFrame, movies: DataFrame, host_name: str, mode: str):
    win = data.withWatermark("date", "1 day") \
    .groupBy(
        window("date", "30 days"), data.film_id) \
    .agg(
        count("rate").alias("rate_count"),
        sum("rate").alias("rate_sum"),
        approx_count_distinct("user_id").alias("user_count")
    )
    win = win.join(movies, movies.ID == win.film_id)
    win = win.drop("window", "ID", "Year")

    if mode == 'a':
        #Realtime mode A
        query = win.writeStream \
        .outputMode("complete") \
        .foreachBatch (
            lambda batchDF, _:
            batchDF.write
                .mode("overwrite") \
                .format("org.apache.spark.sql.redis") \
                .option("table", "result") \
                .option("key.column", "film_id") \
                .option("checkpointLocation", "/tmp/realtime_checkpoints") \
                .option("host", host_name) \
                .save()
        ) \
        .start()
    
    elif mode == 'c':
        #Realtime mode C
        query = win.writeStream \
        .outputMode("append") \
        .foreachBatch (
            lambda batchDF, _:
            batchDF.write
                .mode("overwrite") \
                .format("org.apache.spark.sql.redis") \
                .option("table", "result") \
                .option("key.column", "film_id") \
                .option("checkpointLocation", "/tmp/realtime_checkpoints") \
                .option("host", host_name) \
                .save()
        ) \
        .start()

def detect_anomalies(data: DataFrame, movies: DataFrame, anomaly_window_length: int, anomaly_min_rate_count: int, anomaly_min_avg_rate: int, host_name: str):
    #Calculate aggregated values
    anomalies_window = data.withWatermark("date", "1 day") \
    .groupBy(window("date", f"{anomaly_window_length} days", "1 day"), data.film_id) \
    .agg(
        count("rate").alias("rate_count"),
        sum("rate").alias("rate_sum")) \
    .select(
        col("film_id"),
        col("rate_count"),
        date_format(col("window").start, "dd.MM.yyyy").alias("window_start"),
        date_format(col("window").end, "dd.MM.yyyy").alias("window_end"),
        (col("rate_sum") / col("rate_count")).alias("avg_rate")
    )

    #Check anomaly conditions
    anomalies = anomalies_window.where(
        (anomalies_window.rate_count >= anomaly_min_rate_count) &
        (anomalies_window.avg_rate >= anomaly_min_avg_rate)
    ) \
    .join(movies, movies.ID == anomalies_window.film_id) \
    .drop("ID", "Year", "film_id")

    #Format results for Kafka output
    anomalies_formatted = anomalies.select(concat(
        col("Title"),
        lit(","),
        col("rate_count"),
        lit(","),
        col("avg_rate"),
        lit(","),
        col("window_start"),
        lit(","),
        col("window_end")
    ).alias("value"))

    #Write output to Kafka
    anomalies_output = anomalies_formatted.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host_name}:9092") \
    .option("topic", "prj-2-anomalies") \
    .option("checkpointLocation", "/tmp/anomaly_checkpoints") \
    .outputMode("append") \
    .start()


def main():
    #Handle input parameters
    if len(sys.argv) < 5:
        print("Not enough arguments")
        return
    mode = sys.argv[1]
    anomaly_window_length = int(sys.argv[2])
    anomaly_min_rate_count = int(sys.argv[3])
    anomaly_min_avg_rate = int(sys.argv[4])
    print(f"Arguments: {mode}, {anomaly_window_length}, {anomaly_min_rate_count}, {anomaly_min_avg_rate}")

    #Create Spark sesion
    spark = SparkSession.builder \
    .appName("BigData Netflix") \
    .getOrCreate()

    #Create data sources
    host_name = socket.gethostname()
    source = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{host_name}:9092") \
    .option("subscribe", "prj-2-input") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()
    data = source.select(expr("CAST(value AS STRING)").alias("value"))

    movies = spark.read.option("header", True).csv("gs://pojemnik/projekt2/movie_titles.csv")

    #Format and clean input
    split_columns = split(data["value"], ",")

    data = data.withColumn("date", to_timestamp(split_columns[0], "yyyy-MM-dd")) \
    .withColumn("film_id", split_columns[1]) \
    .withColumn("user_id", split_columns[2]) \
    .withColumn("rate", split_columns[3].cast(IntegerType()))
    data = data.drop("value")

    process_realtime_data(data, movies, host_name, mode)
    detect_anomalies(data, movies, anomaly_window_length, anomaly_min_rate_count, anomaly_min_avg_rate, host_name)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
