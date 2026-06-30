import os
# from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, window, concat_ws, date_format
from pyspark.sql.types import StructField, StructType, StringType

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

input_topic = os.environ["TOPIC_RAW"]
output_topic = os.environ["TOPIC_AGGREGATES"]
################################################


################################################
# Read from Kafka
########################
movies_raw = ( 
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

movie_schema = StructType(
    [
        StructField("movieId", StringType()),
        StructField("rating", StringType()),
        StructField("datetime", StringType()),
        StructField("longitude", StringType()),
        StructField("latitude", StringType()),
        StructField("city", StringType()),
    ]
)

movies = (
    movies_raw.select(
        from_json(col("value").cast("string"), movie_schema).alias("payload"),
        col("timestamp").alias("ingest_timestamp"),
    )
    .select("payload.*", "ingest_timestamp")
)
################################################

aggregated = (
    movies
      .withWatermark("ingest_timestamp", "10 minutes")
      .groupBy(
          window(col("ingest_timestamp"), "1 minutes", "1 minutes"),
          col("city")
      )
      .count()
      .withColumnRenamed("count", "ratings_per_minute")
)

output_df = (
    aggregated.select(
        concat_ws(
            "|",
            col("city"),
            date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss")
        ).alias("key"),
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
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
  .option("checkpointLocation", "/tmp/checkpoint005")
  .outputMode("update") # append
  .start()
)

query.awaitTermination()