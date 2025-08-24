import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, count, broadcast, concat_ws, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from elasticsearch import Elasticsearch, exceptions

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LogsAggsToElasticsearch") \
    .config("spark.sql.streaming.checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/checkpoint/") \
    .getOrCreate()

# JDBC properties
jdbc_url = "jdbc:postgresql://10.0.3.216:5432/test_db"
jdbc_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Read static data from PostgreSQL
car_model_df = spark.read.jdbc(url=jdbc_url, table="cars", properties=jdbc_properties)
car_model_df = car_model_df.withColumnRenamed("VIN Number", "VIN")

# Broadcast the static DataFrame
broadcast_car_model_df = broadcast(car_model_df)

# Elasticsearch configuration
es_host = "10.0.3.216"
es_port = 9200
es_scheme = "http"
es_index = "aggs2_data"

# Create Elasticsearch client
es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

# Elasticsearch index mapping
joined_aggs_mappings = {
    "properties": {
        "window_start": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss'Z'"},
        "window_end": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss'Z'"},
        "CarModel": {"type": "keyword"},
        "count": {"type": "long"}
    }
}

def create_index_if_not_exists(es_client, index_name, mappings):
    try:
        if not es_client.indices.exists(index=index_name):
            print(f"Index '{index_name}' does not exist. Creating index...")
            es_client.indices.create(index=index_name, mappings=mappings)
            print(f"Index '{index_name}' created successfully.")
        else:
            print(f"Index '{index_name}' already exists.")
    except exceptions.RequestError as e:
        print(f"RequestError: {e.info}")
    except exceptions.ConnectionError as e:
        print(f"ConnectionError: {e}")
    except Exception as e:
        print(f"Error creating index: {e}")

# Create the Elasticsearch index
create_index_if_not_exists(es, es_index, joined_aggs_mappings)

# Define the schema for Kafka messages
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("VIN", StringType(), True),
    StructField("InternalParameter", StructType([
        StructField("ParameterName", StringType(), True),
        StructField("ParameterValue", IntegerType(), True),
        StructField("ParameterUnit", StringType(), True)
    ]), True)
])

# Kafka parameters
kafka_bootstrap_servers = "b-2.kafkaaggs2.dzsjnj.c3.kafka.eu-central-1.amazonaws.com:9092,b-1.kafkaaggs2.dzsjnj.c3.kafka.eu-central-1.amazonaws.com:9092"
kafka_topic = "topic1"

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from Kafka to a string
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data using the schema
kafka_df = kafka_df.select(from_json(col("value"), schema).alias("data"))

# Flatten the DataFrame and ensure timestamp is of TimestampType
kafka_df = kafka_df.select(
    col("data.timestamp").alias("timestamp"),
    col("data.VIN"),
    col("data.InternalParameter.ParameterName"),
    col("data.InternalParameter.ParameterValue"),
    col("data.InternalParameter.ParameterUnit")
)

# Perform the join with the Kafka stream DataFrame
joined_df = kafka_df.join(broadcast_car_model_df, kafka_df.VIN == broadcast_car_model_df.VIN, "left_outer") \
    .select(
        kafka_df.timestamp,
        broadcast_car_model_df["Car Model"].alias("CarModel") 
    )

# Apply aggregations with a 1-hour window
aggregated_df = joined_df \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(
        window("timestamp", "1 hour"),
        col("CarModel")
    ) \
    .agg(count("*").alias("count"))

# Flatten the window column and create a composite ID
flat_aggregated_df = aggregated_df.select(
    date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_start"),
    date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_end"),
    col("CarModel"),
    col("count"),
    concat_ws("_", 
        date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),  
        col("CarModel")
    ).alias("id")
)

es_write_conf = {
    "es.nodes": es_host,
    "es.port": str(es_port),
    "es.index.auto.create": "false",
    "es.mapping.id": "id",
    "es.write.operation": "upsert"   
}

def process_batch(batch_df: DataFrame, batch_id: int):
    try:
        print(f"Processing batch: {batch_id}")

        # Display schema of the batch DataFrame
        print("Schema of the batch DataFrame:")
        batch_df.printSchema()

        # Display sample data from the batch DataFrame
        print("Sample data from the batch DataFrame:")
        batch_df.show(5, truncate=False)

        # Write to Elasticsearch
        if batch_df.count() > 0:
            batch_df.write \
                .format("org.elasticsearch.spark.sql") \
                .options(**es_write_conf) \
                .mode("append") \
                .save(es_index)
        else:
            print("No data available in this batch.")
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")
        import traceback
        print(traceback.format_exc())

# Write the aggregated data to Elasticsearch with a 5-second trigger interval
query = flat_aggregated_df.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='10 seconds') \
    .start()

# Await termination of the stream
query.awaitTermination()
