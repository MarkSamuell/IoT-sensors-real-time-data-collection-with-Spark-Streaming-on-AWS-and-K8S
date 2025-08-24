from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialize Spark session with additional configurations
spark = SparkSession.builder \
    .appName("ReadFromPublicMSK") \
    .config("spark.kafka.connection.timeout", "60s") \
    .config("spark.kafka.request.timeout.ms", "120000") \
    .config("spark.kafka.admin.timeout", "120s") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "b-2-public.kafkapublic.cp877p.c3.kafka.eu-central-1.amazonaws.com:9198,b-1-public.kafkapublic.cp877p.c3.kafka.eu-central-1.amazonaws.com:9198"
kafka_topic = "test-topic"

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
    .option("kafka.request.timeout.ms", "120000") \
    .option("kafka.session.timeout.ms", "60000") \
    .option("failOnDataLoss", "false") \
    .load()

# Print schema to verify the DataFrame is created correctly
kafka_df.printSchema()

# Simple processing: just select the value and convert it to string
processed_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Write the processed data to console (for testing)
query = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()