from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaToIceberg") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.demo.warehouse", "s3://aws-emr-studio-381492251123-eu-central-1/ICEBERG/") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.streaming.checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/") \
    .getOrCreate()

# Define the schema
ioc_parameters_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("InternalParameter", StructType([
        StructField("ParameterName", StringType(), True),
        StructField("ParamterType", StringType(), True),
        StructField("ParameterValue", StringType(), True),
        StructField("ParameterUnit", StringType(), True)
    ]), True)
])

schema = StructType([
    StructField("SEV_ID", StringType(), True),
    StructField("VinNumber", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Name", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("SEV_Msg", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("NetworkType", StringType(), True),
    StructField("NetworkID", StringType(), True),
    StructField("IoC", StructType([
        StructField("Parameters", ArrayType(ioc_parameters_schema), True)
    ]), True)
])

# Kafka parameters
kafka_bootstrap_servers = "b-1.kafkaprivatecluster.2vrw32.c3.kafka.eu-central-1.amazonaws.com:9092,b-2.kafkaprivatecluster.2vrw32.c3.kafka.eu-central-1.amazonaws.com:9092"
kafka_topic = "sevs_topic2"

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from Kafka to a string and parse the JSON data
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Flatten the structure for easier querying
flattened_df = parsed_df.select(
    col("VinNumber"),
    col("SEV_ID"),
    # Convert the TimeStamp from string to timestamp
    to_timestamp(from_unixtime(col("TimeStamp").cast("long"))).alias("TimeStamp")
)

# Add a watermark on the TimeStamp column
watermarked_df = flattened_df.withWatermark("TimeStamp", "10 minutes")

# Perform hourly aggregation
aggregated_df = watermarked_df \
    .withWatermark("Timestamp", "1 hour") \
    .groupBy(
        window("Timestamp", "1 hour"),
        "VinNumber"
    ) \
    .agg(count("SEV_ID").alias("SEV_Count"))

def write_to_iceberg(batch_df, batch_id):
    try:
        print(f"Processing batch {batch_id}")
        print(f"Batch size: {batch_df.count()} records")
        batch_df.show(5, truncate=False)

        # Rename the window start and end columns
        batch_df = batch_df.select(
            col("window.start").alias("WindowStart"),
            col("window.end").alias("WindowEnd"),
            "VinNumber",
            "SEV_Count"
        )

        # Read the current Iceberg table
        current_table = spark.table("demo.uraues_db.sevs_agg2")

        # Perform the merge operation using DataFrame APIs
        merged_df = current_table.alias("target").join(
            batch_df.alias("source"),
            (col("target.WindowStart") == col("source.WindowStart")) &
            (col("target.WindowEnd") == col("source.WindowEnd")) &
            (col("target.VinNumber") == col("source.VinNumber")),
            "full_outer"
        ).select(
            coalesce(col("source.WindowStart"), col("target.WindowStart")).alias("WindowStart"),
            coalesce(col("source.WindowEnd"), col("target.WindowEnd")).alias("WindowEnd"),
            coalesce(col("source.VinNumber"), col("target.VinNumber")).alias("VinNumber"),
            when(col("source.SEV_Count").isNotNull() & col("target.SEV_Count").isNotNull(),
                 greatest(col("source.SEV_Count"), col("target.SEV_Count")))
            .otherwise(coalesce(col("source.SEV_Count"), col("target.SEV_Count")))
            .alias("SEV_Count")
        )

        # Log the merge results
        print("Merged DataFrame:")
        merged_df.show(10, truncate=False)
        print(f"Merged DataFrame count: {merged_df.count()}")

        # Write the merged data back to the Iceberg table
        merged_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("demo.uraues_db.sevs_agg2")

        print(f"Batch {batch_id}: Merged {batch_df.count()} records into the Iceberg table.")

        # Verify the write operation
        updated_table = spark.table("demo.uraues_db.sevs_agg2")
        print("Updated Iceberg table:")
        updated_table.show(10, truncate=False)
        print(f"Updated Iceberg table count: {updated_table.count()}")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")

# Write the aggregated data to Iceberg
query = aggregated_df.writeStream \
    .foreachBatch(write_to_iceberg) \
    .outputMode("update") \
    .trigger(processingTime='10 seconds') \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()