import time
import uuid
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, concat, lit, when, udf, count, sum as sum_, window, to_timestamp, date_format, coalesce
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "b-2.kafakprivate.fboe6j.c3.kafka.eu-central-1.amazonaws.com:9092,b-1.kafakprivate.fboe6j.c3.kafka.eu-central-1.amazonaws.com:9092"
KAFKA_TOPIC = "logs-topic"
ES_HOST = "10.0.3.216"
ES_PORT = 9200
ES_SCHEME = "http"
ES_INDEX = "test_event_logs_aggs"
CHECKPOINT_LOCATION = "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/checkpoint/logs_aggs_poc033/"
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
            "VIN": {"type": "keyword"},
            "Signal_Name": {"type": "keyword"},
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
    StructField("Campaign_ID", StringType(), True),
    StructField("DataEntries", ArrayType(StructType([
        StructField("DataName", StringType(), True),
        StructField("DataType", StringType(), True),
        StructField("DataValue", StringType(), True),
        StructField("TimeStamp", StringType(), True)
    ])), True),
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
        logger.info(f"Elasticsearch bulk update: {success} succeeded, {failed} failed")
        
        if failed > 0:
            logger.error(f"Failed to update {failed} documents")
        
    except Exception as e:
        logger.error(f"Error during Elasticsearch bulk update: {str(e)}")
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
    parsed_df = batch_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    flattened_df = parsed_df.select(
        col("data.VinNumber").alias("VIN"),
        explode("data.DataEntries").alias("DataEntry")
    ).select(
        col("VIN"),
        col("DataEntry.DataName").alias("Signal_Name"),
        to_timestamp(col("DataEntry.TimeStamp").cast("double") / 1000).alias("Timestamp")
    )

    # Convert the broadcasted list back to DataFrame
    cars_signals = spark.createDataFrame(broadcast_cars_signals.value)

    # Perform the join and resolve column ambiguity using aliases
    joined_df = flattened_df.alias("batch").join(
        cars_signals.alias("cs"),
        col("batch.Signal_Name") == col("cs.signal_id"),
        "left_outer"
    )

    # Apply aggregations with a 15-minute window
    aggregated_df = joined_df \
        .withWatermark("Timestamp", "15 minutes") \
        .groupBy(
            window("Timestamp", "15 minutes"),
            col("VIN"),
            col("cs.db_signal_name")
        ) \
        .agg(count("*").alias("count"))

    # Prepare data for Elasticsearch
    flat_aggregated_df = aggregated_df.select(
        date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
        date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
        col("VIN"),
        col("cs.db_signal_name").alias("Signal_Name"),
        col("count")
    )

    # Generate ID for upsert (without count)
    flat_aggregated_df_with_id = flat_aggregated_df.withColumn(
        "es_id",
        generate_uuid5_udf(concat(
            col("VIN"), 
            col("window_start"),
            col("Signal_Name")
        ))
    )


    logger.info(f"Batch {batch_id}: Final aggregated count: {flat_aggregated_df_with_id.count()}")
    logger.info(f"Batch {batch_id}: Total count sum: {flat_aggregated_df_with_id.agg({'count': 'sum'}).collect()[0][0]}")
    
    # Log a sample of the final data
    logger.info("Sample of data being sent to Elasticsearch:")
    flat_aggregated_df_with_id.show(5, truncate=False)

    # Write to Elasticsearch
    write_to_elasticsearch(flat_aggregated_df_with_id)

def main():
    global spark, broadcast_cars_signals

    # Create Spark session
    spark = SparkSession.builder \
        .appName("LogsAggsToElasticsearch") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .getOrCreate()

    # Create Elasticsearch index
    es = create_es_client()
    create_index_if_not_exists(es, ES_INDEX, aggs_mappings)

    # Load cars_signals data
    cars_signals_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://10.0.3.216:5432/test_db") \
        .option("dbtable", "cars_signals") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .load()

    cars_signals_df = cars_signals_df.withColumnRenamed("signal_name", "db_signal_name")
    broadcast_cars_signals = spark.sparkContext.broadcast(cars_signals_df.collect())

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