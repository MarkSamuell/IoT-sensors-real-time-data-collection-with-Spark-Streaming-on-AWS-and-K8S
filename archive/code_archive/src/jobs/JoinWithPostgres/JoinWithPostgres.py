import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, sha2, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from elasticsearch import Elasticsearch, exceptions

# Create SparkSession
spark = SparkSession.builder \
    .appName("JoinToElasticsearch") \
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

# Elasticsearch configuration
es_host = "10.0.3.216"
es_port = 9200
es_scheme = "http"
es_index = "joined_raw_index4"

joined_raw_mappings = {
    "properties": {
        "timestamp": {"type": "date"},
        "VIN": {"type": "keyword"},
        "Car Model": {"type": "keyword"},
        "ParameterName": {"type": "keyword"},
        "ParameterValue": {"type": "keyword"},
        "ParameterUnit": {"type": "keyword"}
    }
}

# Create Elasticsearch client
es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

def create_index_if_not_exists(es_client, index_name, mappings):
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body={"mappings": mappings})
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Index '{index_name}' already exists.")

# Create the index in Elasticsearch
create_index_if_not_exists(es, es_index, joined_raw_mappings)

# Define the schema
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("VIN", StringType(), True),
    StructField("InternalParameter", StructType([
        StructField("ParameterName", StringType(), True),
        StructField("ParameterValue", StringType(), True),
        StructField("ParameterUnit", StringType(), True)
    ]), True)
])

# Kafka parameters
kafka_bootstrap_servers = "b-1.kafkacluster.w0r2xo.c3.kafka.eu-central-1.amazonaws.com:9092,b-2.kafkacluster.w0r2xo.c3.kafka.eu-central-1.amazonaws.com:9092"
kafka_topic = "topic1"

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data using the schema
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Flatten the DataFrame
flattened_df = parsed_df.select(
    col("timestamp"),
    col("VIN"),
    col("InternalParameter.ParameterName"),
    col("InternalParameter.ParameterValue"),
    col("InternalParameter.ParameterUnit")
)

# Broadcast the static DataFrame
broadcast_car_model_df = spark.sparkContext.broadcast(car_model_df.collect())

def join_with_broadcast(batch_df, batch_id):
    # Convert broadcast variable to DataFrame
    car_model_df = spark.createDataFrame(broadcast_car_model_df.value)
    
    # Perform the join
    joined_df = batch_df.join(car_model_df, "VIN", "left_outer")
    
    # Generate unique ID using SHA256 hash
    es_df = joined_df.select(
        sha2(concat(col("VIN"), col("timestamp"), col("ParameterName")), 256).alias("id"),
        col("timestamp"),
        col("VIN"),
        col("Car Model"),
        col("ParameterName"),
        col("ParameterValue"),
        col("ParameterUnit")
    )
    
    # Write to Elasticsearch
    es_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", es_host) \
        .option("es.port", es_port) \
        .option("es.resource", es_index) \
        .option("es.mapping.id", "id") \
        .option("es.write.operation", "upsert") \
        .mode("append") \
        .save()

# Process the stream
query = flattened_df.writeStream \
    .foreachBatch(join_with_broadcast) \
    .start()

# Await termination of the stream
query.awaitTermination()