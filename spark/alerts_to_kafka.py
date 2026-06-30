import os
# from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, window, concat_ws, date_format
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession.builder.appName("BigDataProject").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

################################################
# Credentials
########################
read_bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
read_username = os.environ["KAFKA_API_KEY"]
read_password = os.environ["KAFKA_API_SECRET"]

write_bootstrap_servers = read_bootstrap_servers
write_username = read_username
write_password = read_password

input_topic = os.environ["TOPIC_AGGREGATES"]
output_topic = os.environ["TOPIC_ALERTS"]
################################################


################################################
# Read from Kafka
########################
movies_aggregated = ( 
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", read_bootstrap_servers)
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(read_username, read_password))
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("subscribe", input_topic)
    .option("startingOffsets", "earliest")
    .load()
)

movie_aggregate_schema = StructType(
    [
        StructField("window_start", StringType()),
        StructField("window_end", StringType()),
        StructField("city", StringType()),
        StructField("ratings_per_minute", IntegerType()),
    ]
)

movies_agg = (
    movies_aggregated.select(
        from_json(col("value").cast("string"), movie_aggregate_schema).alias("payload"),
        col("timestamp").alias("ingest_timestamp"),
    )
    .select("payload.*", "ingest_timestamp")
)
################################################

alerts = (
    movies_agg
      .withWatermark("ingest_timestamp", "10 minutes")
      .where((col("ratings_per_minute") > 20) & (col("city") != "Unknown"))
      .dropDuplicatesWithinWatermark(["city", "window_start"])
)
# --> we don't want to trigger an alert twice for the same city + timewindow 
# --> we want to trigger an alert only once, then just update the ratings_per_minute, by dropping the duplicate (city+timewindow)

output_df = (
    alerts.select(
        concat_ws(
            "|",
            col("city"),
            date_format(col("window_start"), "yyyy-MM-dd HH:mm:ss")
        ).alias("key"),
        to_json(struct(
            col("window_start"),
            col("window_end"),
            col("city"),
            col("ratings_per_minute")
        )).alias("value")
    )
    .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
)

query = (output_df
  .writeStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", write_bootstrap_servers)
  .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(write_username, write_password))
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
   .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("topic", output_topic)
  .option("checkpointLocation", "/tmp/checkpoint006")
  .outputMode("append") # update
  .start()
)

query.awaitTermination()