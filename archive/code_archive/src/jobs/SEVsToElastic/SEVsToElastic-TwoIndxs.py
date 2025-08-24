import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, broadcast, to_json, struct, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, MapType, ArrayType
from elasticsearch import Elasticsearch, exceptions

# Elasticsearch configuration
es_host = "10.0.3.216"
es_port = 9200
es_scheme = "http"
es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

# Define mappings for both indices
security_events_mappings = {
    "properties": {
        "SEV_ID": {"type": "keyword"},
        "VIN": {"type": "keyword"},
        "Timestamp": {"type": "text"},
        "Name": {"type": "text"},
        "Type": {"type": "text"},
        "Severity": {"type": "keyword"},
        "NetworkType": {"type": "keyword"},
        "NetworkID": {"type": "keyword"},
        "SEV_Msg": {"type": "text"},
        "Origin": {"type": "keyword"}
    }
}

# Update the sevs_logs_mappings
sevs_logs_mappings = {
    "properties": {
        "SEV_ID": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "VIN": {"type": "keyword"},
        "ParameterName": {"type": "keyword"},
        "ParameterValue": {"type": "keyword"},
        "ParameterUnit": {"type": "keyword"}
    }
}

def create_index_if_not_exists(es_client, index_name, mappings):
    try:
        if not es_client.indices.exists(index=index_name):
            print(f"Index '{index_name}' does not exist. Creating index...")
            es_client.indices.create(
                index=index_name,
                body={"mappings": mappings}
            )
            print(f"Index '{index_name}' created successfully.")
        else:
            print(f"Index '{index_name}' already exists.")
    except exceptions.RequestError as e:
        print(f"RequestError: {e.info}")
    except exceptions.ConnectionError as e:
        print(f"ConnectionError: {e}")
    except Exception as e:
        print(f"Error creating index: {e}")

# Create both indices in Elasticsearch
create_index_if_not_exists(es, "security_events", security_events_mappings)
create_index_if_not_exists(es, "sevs_logs", sevs_logs_mappings)

# Create SparkSession
spark = SparkSession.builder \
    .appName("SEVsToElasticsearch") \
    .config("spark.sql.streaming.checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/checkpoint/") \
    .getOrCreate()

# Define the schema
ioc_parameters_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("VIN", StringType(), True),
    StructField("InternalParameter", StructType([
        StructField("ParameterName", StringType(), True),
        StructField("ParameterValue", StringType(), True),
        StructField("ParameterUnit", StringType(), True)
    ]), True)
])

schema = StructType([
    StructField("SEV_ID", StringType(), True),
    StructField("VIN", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Name", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("NetworkType", StringType(), True),
    StructField("NetworkID", StringType(), True),
    StructField("SEV_Msg", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("IoC", StructType([
        StructField("Parameters", ArrayType(ioc_parameters_schema), True)
    ]), True)
])

# Kafka parameters
kafka_bootstrap_servers = "b-1.kafkacluster.w0r2xo.c3.kafka.eu-central-1.amazonaws.com:9092,b-2.kafkacluster.w0r2xo.c3.kafka.eu-central-1.amazonaws.com:9092"
kafka_topic = "topic2"

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

# Flatten the DataFrame for security events
security_events_df = kafka_df.select(
    col("data.SEV_ID"),
    col("data.VIN"),
    col("data.Timestamp"),
    col("data.Name"),
    col("data.Type"),
    col("data.Severity"),
    col("data.NetworkType"),
    col("data.NetworkID"),
    col("data.SEV_Msg"),
    col("data.Origin")
)

# Process IoC logs
sevs_logs_df = kafka_df.select(
    col("data.SEV_ID"),
    explode(col("data.IoC.Parameters")).alias("Parameter")
)

# Flatten the Parameter structure while keeping the SEV_ID
sevs_logs_df = sevs_logs_df.select(
    col("SEV_ID"),
    col("Parameter.timestamp"),
    col("Parameter.VIN"),
    col("Parameter.InternalParameter.ParameterName"),
    col("Parameter.InternalParameter.ParameterValue"),
    col("Parameter.InternalParameter.ParameterUnit")
)

# Elasticsearch configuration for Spark
es_write_conf = {
    "es.nodes": es_host,
    "es.port": str(es_port),
    "es.index.auto.create": "false"
}

# Write security events to Elasticsearch
security_events_query = security_events_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .options(**es_write_conf) \
    .option("es.resource", "security_events") \
    .option("es.mapping.id", "SEV_ID") \
    .outputMode("append") \
    .start()

# Write IoC logs to Elasticsearch
sevs_logs_query = sevs_logs_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .options(**es_write_conf) \
    .option("es.resource", "sevs_logs") \
    .outputMode("append") \
    .start()

# Await termination of both streams
security_events_query.awaitTermination()
sevs_logs_query.awaitTermination()
