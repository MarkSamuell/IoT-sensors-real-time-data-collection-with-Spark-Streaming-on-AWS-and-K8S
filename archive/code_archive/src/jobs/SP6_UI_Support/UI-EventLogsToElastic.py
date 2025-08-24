import time
import uuid
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, concat, lit, when, udf, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, LongType, BooleanType

# Fixed namespace for UUID5
FIXED_NAMESPACE = uuid.UUID('32f51344-0933-43d2-b923-304b8d8f7896')

# Global variables
total_messages_received = 0
total_event_logs = 0
time_sent_to_kafka = None
time_received_from_kafka = None
time_inserted_to_elasticsearch = None
is_error = False
error_type = ""
error_message = ""

# Elasticsearch configuration
es_host = "10.0.3.216"
es_port = 9200
es_scheme = "http"
raw_es_index = "ui_event_logs"
ELASTICSEARCH_BATCH_LOGS_INDEX = "logging_event_logs"

# Elasticsearch configuration for Spark
es_write_conf = {
    "es.nodes": es_host,
    "es.port": str(es_port),
    "es.resource": f"{raw_es_index}",
    "es.mapping.id": "Log_ID",
    "es.write.operation": "upsert"
}

raw_mappings = {
    "properties": {
        "Log_ID": {"type": "keyword"},
        "Timestamp": {
            "type": "date",
            "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        },
        "Signal_Name": {"type": "keyword"},
        "Signal_Value": {"type": "keyword"},
        "Signal_Unit": {"type": "keyword"},
        "VIN": {"type": "keyword"},
        "Vehicle_Model": {"type": "keyword"},
        "Fleet_Name": {"type": "keyword"},
        "Campaign_Id": {"type": "keyword"},
        "Component": {"type": "keyword"},
        "Datatype": {"type": "keyword"},
        "Min": {"type": "float"},
        "Max": {"type": "float"},
        "Signal_Fully_Qualified_Name": {"type": "keyword"}
    }
}

# Create Elasticsearch client
es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

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

# Create the index in Elasticsearch
create_index_if_not_exists(es, raw_es_index, raw_mappings)

# Define the schema
schema = StructType([
    StructField("Campaign_ID", StringType(), True),
    StructField("DataEntries", ArrayType(StructType([
        StructField("DataName", StringType(), True),
        StructField("DataType", StringType(), True),
        StructField("DataValue", StringType(), True),
        StructField("TimeStamp", StringType(), True)  # Epoch time in milliseconds as string
    ])), True),
    StructField("VinNumber", StringType(), True)
])

# Kafka parameters
kafka_bootstrap_servers = "b-1.kafkaprivatecluster.m3g24p.c3.kafka.eu-central-1.amazonaws.com:9092,b-2.kafkaprivatecluster.m3g24p.c3.kafka.eu-central-1.amazonaws.com:9092"
kafka_topic = "logs-topic"

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaToElasticsearch") \
    .config("spark.sql.streaming.checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/checkpoint/logs02/") \
    .getOrCreate()

# Load cars_signals data
cars_signals_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.3.216:5432/test_db") \
    .option("dbtable", "cars_signals") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()

cars_signals_df = cars_signals_df.withColumnRenamed("signal_name", "db_signal_name")

# Load fleet_cars data
fleet_cars_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://10.0.3.216:5432/test_db") \
    .option("dbtable", "fleet_cars") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()

# Create a DataFrame representing the stream of input lines from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Define a UDF for UUID5 generation
def generate_uuid5(name):
    if name is None:
        return None
    return str(uuid.uuid5(FIXED_NAMESPACE, str(name)))

generate_uuid5_udf = udf(generate_uuid5, StringType())

# Convert the value column from Kafka to a string and parse JSON
parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Broadcast cars_signals and fleet_cars DataFrames
broadcast_cars_signals = spark.sparkContext.broadcast(cars_signals_df.collect())
broadcast_fleet_cars = spark.sparkContext.broadcast(fleet_cars_df.collect())

def batch_logger(spark, batch_id, es_index=ELASTICSEARCH_BATCH_LOGS_INDEX):
    global time_sent_to_kafka, time_received_from_kafka, time_inserted_to_elasticsearch, \
           total_messages_received, total_event_logs, is_error, error_type, error_message

    log_data = [
        (
            batch_id,
            time_sent_to_kafka,
            time_received_from_kafka,
            time_inserted_to_elasticsearch,
            total_messages_received,
            total_event_logs,
            is_error,
            error_type,
            error_message
        )
    ]
    
    log_schema = StructType([
        StructField("batch_number", LongType(), False),
        StructField("time_sent_to_kafka", StringType(), True),
        StructField("time_received_from_kafka", StringType(), True),
        StructField("time_inserted_to_elasticsearch", StringType(), True),
        StructField("messages_received_from_kafka", LongType(), False),
        StructField("event_logs", LongType(), False),
        StructField("is_error", BooleanType(), False),
        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    log_df = spark.createDataFrame(log_data, log_schema)
    
    es_write_conf = {
        "es.nodes": es_host,
        "es.port": str(es_port),
        "es.resource": es_index,
        "es.write.operation": "index"
    }
    
    try:
        log_df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_conf) \
            .mode("append") \
            .save()
        
        print(f"Successfully logged batch {batch_id} information to Elasticsearch index '{es_index}'")
    except Exception as e:
        print(f"Error logging batch information to Elasticsearch: {e}")
    
    is_error = False
    error_type = ""
    error_message = ""

# Function to process each batch
def process_batch(batch_df, batch_id):
    global total_messages_received, total_event_logs, time_sent_to_kafka, time_received_from_kafka, time_inserted_to_elasticsearch, \
           is_error, error_type, error_message
    
    time_received_from_kafka = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Count Kafka messages
    kafka_message_count = batch_df.count()
    total_messages_received += kafka_message_count
    print(f"Batch {batch_id}: Received {kafka_message_count} Kafka messages")
    print(f"Total Kafka messages received so far: {total_messages_received}")
    
    # Convert the broadcasted lists back to DataFrames
    cars_signals = spark.createDataFrame(broadcast_cars_signals.value)
    fleet_cars = spark.createDataFrame(broadcast_fleet_cars.value)

    try:
        # Flatten the structure for easier querying
        flattened_df = batch_df.select(
            col("data.VinNumber").alias("VIN"),
            col("data.Campaign_ID").alias("Campaign_Id"),
            explode(col("data.DataEntries")).alias("entry")
        ).select(
            col("VIN"),
            col("Campaign_Id"),
            col("entry.DataName").alias("Signal_Name"),
            col("entry.DataValue").alias("Signal_Value"),
            col("entry.DataType").alias("Datatype"),
            date_format(to_timestamp(col("entry.TimeStamp").cast("double") / 1000), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("Timestamp")
        )
        
        # Extract the earliest timestamp from the batch as time_sent_to_kafka
        time_sent_to_kafka = flattened_df.agg({"Timestamp": "min"}).collect()[0][0]
        # Convert time_sent_to_kafka to the required format
        time_sent_to_kafka = datetime.strptime(time_sent_to_kafka, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
        
        # Perform the join and resolve column ambiguity using aliases
        joined_df = flattened_df.alias("batch").join(
            cars_signals.alias("cs"),
            col("batch.Signal_Name") == col("cs.signal_id"),
            "left_outer"
        ).join(
            fleet_cars.alias("fc"),
            col("batch.VIN") == col("fc.VIN Number"),
            "left_outer"
        )

        # Add missing columns and format data
        final_df = joined_df.select(
            generate_uuid5_udf(concat(col("batch.VIN"), col("batch.Timestamp"), col("batch.Signal_Name"), col("batch.Signal_Value"))).alias("Log_ID"),
            col("batch.Timestamp").alias("Timestamp"), 
            col("cs.db_signal_name").alias("Signal_Name"),
            col("batch.Signal_Value"),
            when(col("batch.Signal_Name") == "101", "%")
            .when(col("batch.Signal_Name") == "102", "Celsius")
            .when(col("batch.Signal_Name") == "103", "km/h")
            .otherwise("").alias("Signal_Unit"),
            col("batch.VIN"),
            col("fc.Car Model").alias("Vehicle_Model"),
            col("fc.Fleet Name").alias("Fleet_Name"),
            col("batch.Campaign_Id"),
            col("cs.component").alias("Component"),
            col("batch.Datatype"),
            col("cs.min_value").alias("Min"),
            col("cs.max_value").alias("Max"),
            col("cs.signal_fully_qualified_name").alias("Signal_Fully_Qualified_Name")
        )
        
        # Write to Elasticsearch
        final_df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_conf) \
            .mode("append") \
            .save()
        
        total_event_logs += final_df.count()
        time_inserted_to_elasticsearch = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    except Exception as e:
        is_error = True
        error_type = "processing_error"
        error_message = str(e)
        print(f"Error processing batch: {e}")
    
    # Log batch information
    batch_logger(spark, batch_id)

# Write data to Elasticsearch
query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='5 seconds') \
    .start()

# Await termination of the stream
query.awaitTermination()