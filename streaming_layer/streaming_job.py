from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .getOrCreate()

spark

# Kafka connection details
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_topic = "Emad_topic" # add topic name
kafka_username = "JUKQQM4ZM632RECA"
kafka_password = "UUkrPuSttgOC0U9lY3ZansNsKfN9fbxZPFwrGxudDrfv+knTD4rCwK+KdIzVPX0D"

# Define schema for the incoming JSON data
schema = StructType() \
    .add("eventType", StringType()) \
    .add("customerId", StringType()) \
    .add("productId", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("metadata", StructType()
        .add("category", StringType())
        .add("source", StringType())
    ) \
    .add("quantity", IntegerType()) \
    .add("totalAmount", DoubleType()) \
    .add("paymentMethod", StringType()) \
    .add("recommendedProductId", StringType()) \
    .add("algorithm", StringType())

# Read data from Kafka topic as a streaming DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

query = json_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/itversity/kafka_data") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/itversity/checkpoints") \
    .start()


query.awaitTermination()

spark.stop()