import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType, LongType

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
kafka_bootstrap_servers = "b-2.kafkaclustericeberg.wa9le5.c3.kafka.eu-central-1.amazonaws.com:9092,b-1.kafkaclustericeberg.wa9le5.c3.kafka.eu-central-1.amazonaws.com:9092"
kafka_topic = "topic8"

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
    col("entry.DataName").alias("Parameter_Name"),
    col("entry.DataType"),
    col("entry.DataValue"),
    # Convert the TimeStamp from string to timestamp
    to_timestamp(from_unixtime(col("entry.TimeStamp").cast("long") / 1000)).alias("TimeStamp")
)

# Add a watermark on the TimeStamp column
watermarked_df = flattened_df.withWatermark("TimeStamp", "10 minutes")

# Perform the aggregation
aggregated_df = watermarked_df.groupBy(
    window(col("TimeStamp"), "30 minutes"),
    col("VinNumber"),
    col("Parameter_Name")
).agg(count("*").alias("Count"))

# Adjust the DataFrame schema to match the Iceberg table schema
result_df = aggregated_df \
    .select(
        col("VinNumber").cast(StringType()).alias("VinNumber"),
        to_timestamp(col("window.start")).alias("Window_Start"),
        to_timestamp(col("window.end")).alias("Window_End"),
        col("Parameter_Name").cast(StringType()).alias("Parameter_Name"),
        col("Count").cast(LongType()).alias("Count")
    )

# Define the foreachBatch function with merge logic
def process_batch(batch_df, batch_id):
    try:
        print(f"Processing batch {batch_id}")
        print(f"Batch size: {batch_df.count()} records")
        batch_df.show(5, truncate=False)
        
        # Read the current Iceberg table
        current_table = spark.table("demo.uraues_db.events_logs_agg7")
        
        # Perform the merge operation using DataFrame APIs
        merged_df = current_table.alias("target").join(
            batch_df.alias("source"),
            (col("target.Window_Start") == col("source.Window_Start")) &
            (col("target.Window_End") == col("source.Window_End")) &
            (col("target.VinNumber") == col("source.VinNumber")) &
            (col("target.Parameter_Name") == col("source.Parameter_Name")),
            "full_outer"
        ).select(
            coalesce(col("source.VinNumber"), col("target.VinNumber")).alias("VinNumber"),
            coalesce(col("source.Window_Start"), col("target.Window_Start")).alias("Window_Start"),
            coalesce(col("source.Window_End"), col("target.Window_End")).alias("Window_End"),
            coalesce(col("source.Parameter_Name"), col("target.Parameter_Name")).alias("Parameter_Name"),
            when(col("source.Count").isNotNull() & col("target.Count").isNotNull(),
                 greatest(col("source.Count"), col("target.Count")))
            .otherwise(coalesce(col("source.Count"), col("target.Count")))
            .alias("Count")
        )
        
        # Log the merge results
        print("Merged DataFrame:")
        merged_df.show(10, truncate=False)
        print(f"Merged DataFrame count: {merged_df.count()}")
        
        # Write the merged data back to the Iceberg table
        merged_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("demo.uraues_db.events_logs_agg7")
        
        print(f"Batch {batch_id}: Merged {batch_df.count()} records into the Iceberg table.")
        
        # Verify the write operation
        updated_table = spark.table("demo.uraues_db.events_logs_agg7")
        print("Updated Iceberg table:")
        updated_table.show(10, truncate=False)
        print(f"Updated Iceberg table count: {updated_table.count()}")
        
    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")

# Write the aggregated data to the Iceberg table using foreachBatch
query = result_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime='10 seconds') \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()