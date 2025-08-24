import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, sha2, concat, udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# Define the updated schema
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

# Kafka parameters
kafka_bootstrap_servers = "b-1.kafkaclustericeberg.q7xozk.c3.kafka.eu-central-1.amazonaws.com:9092,b-2.kafkaclustericeberg.q7xozk.c3.kafka.eu-central-1.amazonaws.com:9092"
kafka_topic = "topic1"

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaToIceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.demo.warehouse", "s3://aws-emr-studio-381492251123-eu-central-1/ICEBERG/") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.streaming.checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/") \
    .getOrCreate()

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from Kafka to a string and parse JSON
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Flatten the structure for easier querying
flattened_df = parsed_df.select(
    col("data.VinNumber"),
    explode(col("data.DataEntries")).alias("entry")
).select(
    col("VinNumber"),
    col("entry.DataName"),
    col("entry.DataValue"),
    col("entry.DataType"),
    col("entry.TimeStamp")
)

# Generate a unique ID using SHA256 hash of VinNumber, TimeStamp, and DataName
flattened_df = flattened_df.withColumn("id", sha2(concat(col("VinNumber"), col("TimeStamp"), col("DataName")), 256))

# Remove any duplicate columns if they exist
flattened_df = flattened_df.dropDuplicates(["id"])

# Write data to Iceberg table
query = flattened_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("demo.uraues_db.raw_data_table")) \
    .start()

# Await termination of the stream
query.awaitTermination()