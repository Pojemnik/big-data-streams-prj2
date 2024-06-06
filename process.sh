$SPARK_HOME/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
--jars jedis-3.9.0.jar,spark-redis_2.12-3.1.0.jar \
process.py \
$1 $2 $3 $4