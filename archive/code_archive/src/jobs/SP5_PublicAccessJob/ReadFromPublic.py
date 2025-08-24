from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ReadFromPublicMSK") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "b-2-public.kafkapublic.cp877p.c3.kafka.eu-central-1.amazonaws.com:9198,b-1-public.kafkapublic.cp877p.c3.kafka.eu-central-1.amazonaws.com:9198"
kafka_topic = "test-topic"

# List all topics in the Kafka cluster
topics_df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "__consumer_offsets") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
    .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;") \
    .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
    .load()

# Extract and print the list of topics
topics_list = topics_df.select("topic").distinct().collect()
print("List of Kafka Topics in the Cluster:")
for topic in topics_list:
    print(topic.topic)

# Define the schema
schema = StructType([
    StructField("Campaign_ID", StringType(), True),
    StructField("DataEntries", ArrayType(StructType([
        StructField("DataName", StringType(), True),
        StructField("DataType", StringType(), True),
        StructField("DataValue", StringType(), True),
        StructField("TimeStamp", StringType(), True)
    ])), True),
    StructField("VinNumber", StringType(), True)
])

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
    .load()

# Convert the value column from Kafka to a string and parse JSON
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Flatten the structure
flattened_df = parsed_df.select(
    col("data.Campaign_ID"),
    col("data.VinNumber"),
    explode("data.DataEntries").alias("DataEntry")
)

final_df = flattened_df.select(
    "Campaign_ID",
    "VinNumber",
    col("DataEntry.DataName"),
    col("DataEntry.DataType"),
    col("DataEntry.DataValue"),
    col("DataEntry.TimeStamp")
)

# Write the data to a CSV file
output_path = "s3://aws-emr-studio-381492251123-eu-central-1/csv_output/"
query = final_df.writeStream \
        .format("csv") \
        .option("header", "true") \
        .option("checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/") \
        .outputMode("append") \
        .start(output_path)

query.awaitTermination()