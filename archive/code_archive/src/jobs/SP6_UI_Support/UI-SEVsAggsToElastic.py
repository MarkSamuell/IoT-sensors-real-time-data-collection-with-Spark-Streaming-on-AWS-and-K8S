import time
import uuid
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, concat, lit, when, udf, to_timestamp, date_format, window, expr, broadcast, coalesce, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "b-2.kafkaprivate.b6fz57.c3.kafka.eu-central-1.amazonaws.com:9092,b-1.kafkaprivate.b6fz57.c3.kafka.eu-central-1.amazonaws.com:9092"
KAFKA_TOPIC = "topic-sevs"
ES_HOST = "10.0.3.216"
ES_PORT = 9200
ES_SCHEME = "http"
ES_INDEX = "test_security_events_aggs"
CHECKPOINT_LOCATION = "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/checkpoint/sevs_aggs_ui/"
FIXED_NAMESPACE = uuid.UUID('32f51344-0933-43d2-b923-304b8d8f7896')

# Global counter
total_messages_received = 0

# Create Elasticsearch client
def create_es_client():
    return Elasticsearch(
        [{'host': ES_HOST, 'port': ES_PORT, 'scheme': ES_SCHEME}],
        verify_certs=False,
        ssl_show_warn=False
    )

# Elasticsearch index mapping
aggs_mappings = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "es_id": {"type": "keyword"},
            "window_start": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
            "window_end": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
            "ID": {"type": "keyword"},
            "SEV_Name": {"type": "keyword"},
            "Severity": {"type": "keyword"},
            "VIN": {"type": "keyword"},
            "Fleet": {"type": "keyword"},
            "count": {"type": "long"}
        }
    }
}

# Create index if not exists
def create_index_if_not_exists(es_client, index_name, mappings):
    try:
        if not es_client.indices.exists(index=index_name):
            logger.info(f"Creating index '{index_name}'...")
            es_client.indices.create(index=index_name, body=mappings)
            logger.info(f"Index '{index_name}' created successfully.")
        else:
            logger.info(f"Index '{index_name}' already exists.")
    except Exception as e:
        logger.error(f"Error creating index: {e}")

# Define schema for Kafka messages
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("IoC", StructType([
        StructField("Parameters", ArrayType(StructType([
            StructField("Timestamp", StringType(), True),
            StructField("InternalParameter", StructType([
                StructField("ParameterName", StringType(), True),
                StructField("ParameterType", StringType(), True),
                StructField("ParameterValue", StringType(), True),
                StructField("ParameterUnit", StringType(), True)
            ]), True)
        ])), True)
    ]), True),
    StructField("NetworkID", StringType(), True),
    StructField("NetworkType", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("SEV_Msg", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("VinNumber", StringType(), True)
])

# Define a UDF for UUID5 generation
def generate_uuid5(name):
    if name is None:
        return None
    return str(uuid.uuid5(FIXED_NAMESPACE, str(name)))

generate_uuid5_udf = udf(generate_uuid5, StringType())

def es_update_action(doc):
    return {
        "_op_type": "update",
        "_index": ES_INDEX,
        "_id": doc["es_id"],
        "script": {
            "source": "ctx._source.count += params.count",
            "lang": "painless",
            "params": {"count": doc["count"]}
        },
        "upsert": doc
    }

def write_to_elasticsearch(df):
    es = create_es_client()
    actions = (es_update_action(row.asDict()) for row in df.collect())
    
    try:
        success, failed = bulk(es, actions, raise_on_error=False, stats_only=True)
        logger.info(f"Elasticsearch bulk update: {success} succeeded, {len(failed) if failed else 0} failed")
        
        if failed:
            for item in failed:
                logger.error(f"Failed to update document: {item}")
        
    except Exception as e:
        logger.error(f"Error during Elasticsearch bulk update: {str(e)}")
        # Optionally, you might want to re-raise the exception here
        raise

    finally:
        es.close()

def process_batch(batch_df, batch_id):
    global total_messages_received
    
    # Count Kafka messages
    kafka_message_count = batch_df.count()
    total_messages_received += kafka_message_count
    logger.info(f"Batch {batch_id}: Received {kafka_message_count} Kafka messages")
    logger.info(f"Total messages received so far: {total_messages_received}")
        
    # Parse and process the data
    parsed_df = batch_df.select(from_json(col("value").cast("string"), ArrayType(schema)).alias("parsed_json"))
    exploded_df = parsed_df.select(explode("parsed_json").alias("event")).select(
        col("event.ID"),
        col("event.VinNumber"),
        col("event.SEV_Msg"),
        col("event.Severity"),
        col("event.Timestamp")
    )
    logger.info(f"Batch {batch_id}: After parsing and exploding: {exploded_df.count()}")

    # Join with fleet_cars and security_event_lookup data
    enriched_df = exploded_df.join(
        broadcast(fleet_cars_df),
        exploded_df.VinNumber == fleet_cars_df["VIN Number"],
        "left_outer"
    ).join(
        broadcast(security_event_lookup_df),
        exploded_df.ID == security_event_lookup_df["sev_id"],
        "left_outer"
    )
    logger.info(f"Batch {batch_id}: After enrichment: {enriched_df.count()}")

    # Convert Timestamp to proper format
    enriched_df = enriched_df.withColumn("Timestamp", 
        date_format(to_timestamp(col("Timestamp").cast("double") / 1000), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )

    # Apply aggregations with a 1-hour window
    aggregated_df = enriched_df \
        .withWatermark("Timestamp", "1 hour") \
        .groupBy(
            window("Timestamp", "1 hour"),
            col("ID"),
            col("sev_name").alias("SEV_Name"),
            col("Severity"),
            col("VinNumber"),
            col("Fleet Name").alias("Fleet")
        ) \
        .count()

    # Flatten the window column and create a composite ID
    flat_aggregated_df = aggregated_df.select(
        date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
        col("ID"),
        col("SEV_Name"),
        col("Severity"),
        col("VinNumber").alias("VIN"),
        col("Fleet"),
        col("count")
    )

    # Generate the es_id for upsert
    flat_aggregated_df_with_id = flat_aggregated_df.withColumn(
        "es_id",
        generate_uuid5_udf(concat(
            coalesce(col("VIN"), lit("")),
            coalesce(col("window_start"), lit("")),
            coalesce(col("ID"), lit("")),
            coalesce(col("SEV_Name"), lit("")),
            coalesce(col("Severity"), lit("")),
        ))
    )
    
    logger.info(f"Batch {batch_id}: Final aggregated count: {flat_aggregated_df_with_id.count()}")
    
    # Log a sample of the final data
    logger.info("Sample of data being sent to Elasticsearch:")
    flat_aggregated_df_with_id.show(5, truncate=False)

    # Write to Elasticsearch
    write_to_elasticsearch(flat_aggregated_df_with_id)

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SevsAggsToElasticsearch") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()

    # Create Elasticsearch index
    es = create_es_client()
    create_index_if_not_exists(es, ES_INDEX, aggs_mappings)

    # Load fleet_cars and security_event_lookup data
    global fleet_cars_df, security_event_lookup_df
    fleet_cars_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.3.216:5432/test_db") \
        .option("dbtable", "fleet_cars") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .load()

    security_event_lookup_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.3.216:5432/test_db") \
        .option("dbtable", "security_event_lookup") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .load()

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafkaConsumer.pollTimeoutMs", 50000000) \
        .load()

    # Process the stream
    query = kafka_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()