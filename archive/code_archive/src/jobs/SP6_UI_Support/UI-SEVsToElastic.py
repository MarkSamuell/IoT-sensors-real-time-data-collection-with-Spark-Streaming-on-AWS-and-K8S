import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_json, struct, explode, concat_ws, 
    when, lit, array, coalesce, size, from_unixtime, count, to_timestamp,
    expr, current_timestamp, date_format, udf, broadcast, lower, trim, min
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, LongType, BooleanType
from elasticsearch import Elasticsearch, exceptions
import logging
import urllib3
import uuid

# Configuration Variables
KAFKA_BOOTSTRAP_SERVERS = "b-1.kafkaprivatecluster.m3g24p.c3.kafka.eu-central-1.amazonaws.com:9092,b-2.kafkaprivatecluster.m3g24p.c3.kafka.eu-central-1.amazonaws.com:9092"
KAFKA_TOPIC = "security-events-topic"
ELASTICSEARCH_SECURITY_EVENTS_INDEX = "ui_security_events"
ELASTICSEARCH_SEVS_LOGS_INDEX = "ui_sevs_logs"
ELASTICSEARCH_BATCH_LOGS_INDEX = "logging_sevs"

FIXED_NAMESPACE = uuid.UUID('32f51344-0933-43d2-b923-304b8d8f7896')

# Disable Elasticsearch client logging
logging.getLogger('elasticsearch').setLevel(logging.WARNING)
logging.getLogger('elastic_transport').setLevel(logging.WARNING)

# Disable urllib3 logging
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging for our application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global counters and variables
total_lists_received = 0
total_security_events = 0
total_ioc_parameters = 0
total_error_records = 0
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

# S3 configuration for error records
s3_error_bucket = "s3://aws-emr-studio-381492251123-eu-central-1/error-records/"

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://10.0.3.216:5432/test_db"
postgres_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Define schemas and mappings
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

security_events_mappings = {
    "properties": {
        "ID": {"type": "keyword"},
        "Alert_ID": {"type": "keyword"},
        "VinNumber": {"type": "keyword"},
        "Timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
        "Severity": {"type": "keyword"},
        "SEV_Msg": {"type": "text"},
        "Origin": {"type": "keyword"},
        "NetworkType": {"type": "keyword"},
        "NetworkID": {"type": "keyword"},
        "SEV_Name": {"type": "keyword"},
        "SEV_Status": {"type": "keyword"},
        "SEV_Desc": {"type": "keyword"},
        "Rule_ID": {"type": "keyword"},
        "Fleet" : {"type": "keyword"},
        "Vehicle_Model" : {"type": "keyword"}
    }
}

sevs_logs_mappings = {
    "properties": {
        "ID": {"type": "keyword"},
        "Alert_ID": {"type": "keyword"},
        "Log_ID": {"type": "keyword"},
        "VinNumber": {"type": "keyword"},
        "Timestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
        "ParameterTimestamp": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
        "ParameterValue": {"type": "keyword"},
        "ParameterUnit": {"type": "keyword"},
        "ParameterType": {"type": "keyword"}
    }
}

# Helper functions
def create_es_client():
    return Elasticsearch(
        [{'host': es_host, 'port': es_port, 'scheme': es_scheme}],
        verify_certs=False,
        ssl_show_warn=False
    )

# Function to get Elasticsearch write configuration
def get_es_write_conf(index_name, id_field):
    try:
        es_client = create_es_client()
        es_info = es_client.info()
        logger.info(f"Elasticsearch info: {es_info}")
        es_client.close()

        nodes = list(es_info.get('nodes', {}).keys())
        if not nodes:
            nodes = [es_host]

        return {
            "es.nodes": ",".join(nodes),
            "es.port": str(es_port),
            "es.resource": index_name,
            "es.mapping.id": id_field,
            "es.write.operation": "upsert"
        }
    except Exception as e:
        logger.error(f"Error getting Elasticsearch configuration: {e}")
        return None

def create_index_if_not_exists(es_client, index_name, mappings):
    try:
        if not es_client.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' does not exist. Creating index...")
            es_client.indices.create(
                index=index_name,
                body={"mappings": mappings}
            )
            logger.info(f"Index '{index_name}' created successfully.")
        else:
            logger.info(f"Index '{index_name}' already exists.")
    except exceptions.RequestError as e:
        logger.error(f"RequestError: {e.info}")
    except exceptions.ConnectionError as e:
        logger.error(f"ConnectionError: {e}")
    except Exception as e:
        logger.error(f"Error creating index: {e}")

def load_postgres_table(spark, table_name):
    try:
        df = spark.read.jdbc(url=postgres_url, table=table_name, properties=postgres_properties)
        logger.info(f"Successfully loaded {table_name} from PostgreSQL")
        return df
    except Exception as e:
        logger.error(f"Error loading {table_name} from PostgreSQL: {e}")
        return None

def log_dataframe_info(df, name):
    logger.info(f"Schema for DataFrame '{name}':")
    df.printSchema()
    logger.info(f"Sample data for DataFrame '{name}':")
    df.show(5)

# Define a UDF for UUID5 generation
def generate_uuid5(name):
    if name is None:
        return None
    return str(uuid.uuid5(FIXED_NAMESPACE, str(name)))

generate_uuid5_udf = udf(generate_uuid5, StringType())

def validate_schema(df, expected_schema):
    expected_columns = set([field.name for field in expected_schema.fields])
    df_columns = set(df.columns)
    missing_columns = list(expected_columns - df_columns)
    
    validation_expr = ' and '.join([f"({col} is not null)" for col in expected_columns])
    df_validated = df.withColumn('is_valid', expr(validation_expr))
    
    return df_validated, missing_columns

def parse_from_kafka(df, schema):
    kafka_json_df = df.withColumn("value", expr("cast(value as string)"))
    parsed_df = kafka_json_df.withColumn("parsed_json", from_json(col("value"), ArrayType(schema)))
    exploded_df = parsed_df.select("value", explode("parsed_json").alias("event"))

    log_dataframe_info(exploded_df, "Exploded DataFrame parsed from Kafka")

    validated_df, missing_columns = validate_schema(exploded_df.select("value", "event.*"), schema)

    if missing_columns:
        logger.warning(f"Schema validation failed. Missing columns: {missing_columns}")

    valid_records = validated_df.filter(col("is_valid")).drop("is_valid")
    invalid_records = validated_df.filter(~col("is_valid")).drop("is_valid")

    error_df = invalid_records.select(
        lit("SCHEMA_FAILED_TO_PARSE").alias("key"),
        col("value"),
        current_timestamp().alias("error_timestamp"),
        lit("schema_validation_error").alias("error_type"),
        lit(', '.join(missing_columns)).alias("error_details")
    )

    log_dataframe_info(valid_records, "Streaming (valid_records) DataFrame (Valid Records)")
    log_dataframe_info(error_df, "Error DataFrame")

    return valid_records, error_df

def process_error_records(error_df, epoch_id):
    global total_error_records
    
    logger.info(f"Processing error records for batch {epoch_id}")
    
    log_dataframe_info(error_df, "Processing Error Records DataFrame")
    
    batch_error_records = error_df.count()
    total_error_records += batch_error_records
    logger.warning(f"Batch {epoch_id}: Error records: {batch_error_records}")
    logger.info(f"Total error records so far: {total_error_records}")
    
    error_df_with_date = error_df.withColumn("date", date_format(col("error_timestamp"), "yyyy-MM-dd"))
    
    try:
        error_df_with_date.write \
            .partitionBy("date", "error_type") \
            .mode("append") \
            .parquet(s3_error_bucket)
        logger.info(f"Successfully wrote {batch_error_records} error records to S3")
    except Exception as e:
        logger.error(f"Error writing error records to S3: {e}")

def enrich_security_events(security_events_df, vehicle_df, vehicle_model_df, fleet_cars_df, security_events_lookup_df):
    try:
        for df_name, df in [("Security Events", security_events_df), ("Vehicle", vehicle_df), 
                            ("Vehicle Model", vehicle_model_df), ("Fleet Cars", fleet_cars_df), 
                            ("Security Events Lookup", security_events_lookup_df)]:
            if df is not None:
                logger.info(f"{df_name} DataFrame schema:")
                df.printSchema()
                logger.info(f"Sample data for {df_name} DataFrame:")
                df.show(5, truncate=False)
            else:
                logger.info(f"{df_name} DataFrame is None")

        enriched_df = security_events_df.withColumnRenamed("ID", "EventID")
        enriched_df = enriched_df.withColumn("SecurityEventVIN", trim(lower(col("VinNumber"))))
        if vehicle_df is not None:
            vehicle_df = vehicle_df.withColumn("VehicleVIN", trim(lower(col("vin"))))

        if vehicle_df is not None and vehicle_model_df is not None:
            enriched_df = enriched_df.join(
                broadcast(vehicle_df),
                enriched_df.SecurityEventVIN == vehicle_df.VehicleVIN,
                "left"
            )
            
            logger.info("After joining with vehicle DataFrame:")
            enriched_df.select("VinNumber", "SecurityEventVIN", "VehicleVIN", "vehicle_model_id").show(5, truncate=False)
            
            enriched_df = enriched_df.join(
                broadcast(vehicle_model_df),
                vehicle_df.vehicle_model_id == vehicle_model_df.id,
                "left"
            )
            
            logger.info("After joining with vehicle_model DataFrame:")
            enriched_df.select("VinNumber", "SecurityEventVIN", "VehicleVIN", "vehicle_model_id", "model_name").show(5, truncate=False)
            
            enriched_df = enriched_df.withColumn(
                "Vehicle_Model",
                coalesce(vehicle_model_df.model_name, lit("Unknown"))
            )
        else:
            enriched_df = enriched_df.withColumn("Vehicle_Model", lit("Unknown"))
            logger.warning("Vehicle or Vehicle Model data unavailable. Setting Vehicle_Model to 'Unknown'.")

        if fleet_cars_df is not None:
            fleet_cars_df = fleet_cars_df.withColumn("FleetVIN", trim(lower(col("VIN Number"))))
            enriched_df = enriched_df.join(
                broadcast(fleet_cars_df),
                enriched_df.SecurityEventVIN == fleet_cars_df.FleetVIN,
                "left"
            ).withColumn("Fleet", coalesce(fleet_cars_df["Fleet Name"], lit("Unknown")))
        else:
            enriched_df = enriched_df.withColumn("Fleet", lit("Unknown"))
            logger.warning("Fleet Cars data unavailable. Setting Fleet to 'Unknown'.")

        if security_events_lookup_df is not None:
            enriched_df = enriched_df.join(
                broadcast(security_events_lookup_df),
                enriched_df.EventID == security_events_lookup_df.sev_id,
                "left"
            ).withColumn("SEV_Name", coalesce(security_events_lookup_df.sev_name, lit("Unknown"))) \
             .withColumn("Rule_ID", coalesce(security_events_lookup_df.rule_id, lit("Unknown"))) \
             .withColumn("SEV_Desc", coalesce(security_events_lookup_df.description, lit("Unknown")))
        else:
            enriched_df = enriched_df \
                .withColumn("SEV_Name", lit("Unknown")) \
                .withColumn("Rule_ID", lit("Unknown")) \
                .withColumn("SEV_Desc", lit("Unknown"))
            logger.warning("Security Events Lookup data unavailable. Setting SEV_Name, Rule_ID, and SEV_Desc to 'Unknown'.")

        enriched_df = enriched_df.withColumn("SEV_Status", lit("unhandled"))


        final_columns = [
            enriched_df.EventID.alias("ID"),
            enriched_df.Alert_ID,
            "VinNumber", "Timestamp", "SEV_Msg", "Severity", "Origin", "NetworkType", "NetworkID",
            "Vehicle_Model", "Fleet", "SEV_Name", "Rule_ID", "SEV_Desc", "SEV_Status"
        ]
        
        final_df = enriched_df.select(final_columns)
        
        logger.info("Final enriched DataFrame schema:")
        final_df.printSchema()
        logger.info("Sample data for final enriched DataFrame:")
        final_df.show(5, truncate=False)
        
        logger.info("Successfully enriched security events data and added SEV_Status")

        return final_df
    except Exception as e:
        logger.error(f"Error enriching security events data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return security_events_df.withColumn("SEV_Status", lit("unhandled")).withColumn("Alert_ID", col("Alert_ID"))

def insert_security_events_to_elasticsearch(df, epoch_id, index_name):
    global total_security_events
   
    logger.info(f"Processing security events batch {epoch_id} for index {index_name}")
   
    log_dataframe_info(df, f"Security Events DataFrame for {index_name}")
    batch_security_events = df.count()
   
    logger.info(f"Batch {epoch_id}: Processing {batch_security_events} unique security events for {index_name}")
   
    if "Alert_ID" not in df.columns:
        logger.error("Alert_ID column is missing from the DataFrame")
        return

    es_write_conf = get_es_write_conf(index_name, id_field='Alert_ID')
   
    try:
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_conf) \
            .mode("append") \
            .save()
       
        total_security_events += batch_security_events
        logger.info(f"Successfully indexed {batch_security_events} security events with SEV_Status 'unhandled' to {index_name}")
       
    except Exception as e:
        logger.error(f"Error processing security events batch for {index_name}: {e}")
        error_df = df.select(
            df["Alert_ID"].alias("key"),
            to_json(struct(*df.columns)).alias("value"),
            current_timestamp().alias("error_timestamp"),
            lit("elasticsearch_write_error").alias("error_type"),
            lit(str(e)).alias("error_details")
        )
        process_error_records(error_df, epoch_id)
   
    logger.info(f"Total unique security events processed so far: {total_security_events}")
    logger.info(f"Finished processing security events batch {epoch_id} for {index_name}")

def insert_ioc_parameters_to_elasticsearch(df, epoch_id, index_name):
    global total_ioc_parameters
   
    logger.info(f"Processing IoC parameters batch {epoch_id} for index {index_name}")
   
    log_dataframe_info(df, f"IoC Parameters DataFrame for {index_name}")
   
    batch_ioc_parameters = df.count()
   
    logger.info(f"Batch {epoch_id}: Processing {batch_ioc_parameters} IoC parameters for {index_name}")
   
    df_with_id = df.withColumn("Log_ID", generate_uuid5_udf(concat_ws("|", *df.columns)))
   
    es_write_conf = get_es_write_conf(index_name, id_field='Log_ID')
   
    try:
        df_with_id.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_conf) \
            .mode("append") \
            .save()
       
        total_ioc_parameters += batch_ioc_parameters
        logger.info(f"Successfully indexed {batch_ioc_parameters} IoC parameters to {index_name}")
       
    except Exception as e:
        logger.error(f"Error processing IoC parameters batch for {index_name}: {e}")
        error_df = df_with_id.select(
            df_with_id["Log_ID"].alias("key"),
            to_json(struct(*df_with_id.columns)).alias("value"),
            current_timestamp().alias("error_timestamp"),
            lit("elasticsearch_write_error").alias("error_type"),
            lit(str(e)).alias("error_details")
        )
        process_error_records(error_df, epoch_id)
       
    logger.info(f"Total IoC parameters processed so far: {total_ioc_parameters}")
    logger.info(f"Finished processing IoC parameters batch {epoch_id} for {index_name}")

def batch_logger(spark, epoch_id, es_index=ELASTICSEARCH_BATCH_LOGS_INDEX):
    global time_sent_to_kafka, time_received_from_kafka, time_inserted_to_elasticsearch, \
           total_lists_received, total_security_events, total_ioc_parameters, \
           is_error, error_type, error_message
    
    log_data = [
        (
            epoch_id,
            time_sent_to_kafka.strftime("%Y-%m-%d %H:%M:%S") if time_sent_to_kafka else None,
            time_received_from_kafka.strftime("%Y-%m-%d %H:%M:%S") if time_received_from_kafka else None,
            time_inserted_to_elasticsearch.strftime("%Y-%m-%d %H:%M:%S") if time_inserted_to_elasticsearch else None,
            total_lists_received,  # total messages_received_from_kafka = total_lists_received
            total_security_events,
            total_ioc_parameters,
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
        StructField("security_events", LongType(), False),
        StructField("ioc", LongType(), False),
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
        
        print(f"Successfully logged batch {epoch_id} information to Elasticsearch index '{es_index}'")
    except Exception as e:
        print(f"Error logging batch information to Elasticsearch: {e}")
    
    is_error = False
    error_type = ""
    error_message = ""

def process_sev_batch(df, epoch_id):
    global total_lists_received, spark, time_sent_to_kafka, time_received_from_kafka, time_inserted_to_elasticsearch, \
           is_error, error_type, error_message
    
    time_sent_to_kafka = df.select(min("timestamp")).collect()[0][0]
    time_received_from_kafka = datetime.now()
    
    log_dataframe_info(df, "Raw Batch DataFrame")
    
    valid_records, error_df = parse_from_kafka(df, schema)
    
    if not error_df.isEmpty():
        is_error = True
        error_type = "parse_error"
        error_message = "Error parsing some records from Kafka"
    
    process_error_records(error_df, epoch_id)
    
    batch_lists_received = df.count()
    total_lists_received += batch_lists_received
    logger.info(f"Batch {epoch_id}: Lists received: {batch_lists_received}")
    logger.info(f"Total lists received so far: {total_lists_received}")
    
    table_dfs = {
        "vehicle": load_postgres_table(spark, "vehicle"),
        "vehicle_model": load_postgres_table(spark, "vehicle_model"),
        "fleet_cars": load_postgres_table(spark, "fleet_cars"),
        "security_event_lookup": load_postgres_table(spark, "security_event_lookup")
    }
    
    failed_tables = [table for table, df in table_dfs.items() if df is None]
    
    if failed_tables:
        is_error = True
        error_type = "postgres_load_error"
        error_message = f"Failed to load the following PostgreSQL tables: {', '.join(failed_tables)}"
        logger.warning(f"{error_message}. Continuing with partial data enrichment.")
    
    valid_records = valid_records.withColumn(
        "Alert_ID", 
        generate_uuid5_udf(
            concat_ws("|", 
                coalesce(col("ID"), lit("")),
                coalesce(col("VinNumber"), lit("")),
                coalesce(col("Timestamp"), lit("")),
                coalesce(col("Severity"), lit(""))
            )
        )
    )

    security_events_df = valid_records.select(
        coalesce(col("ID"), lit("")).alias("ID"),
        coalesce(col("Alert_ID"), lit("")).alias("Alert_ID"),
        coalesce(col("VinNumber"), lit("")).alias("VinNumber"),
        date_format(
            from_unixtime(col("TimeStamp").cast("double") / 1000), 
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        ).alias("Timestamp"),
        coalesce(col("SEV_Msg"), lit("")).alias("SEV_Msg"),
        coalesce(col("Severity"), lit("")).alias("Severity"),
        coalesce(col("Origin"), lit("")).alias("Origin"),
        coalesce(col("NetworkType"), lit("")).alias("NetworkType"),
        coalesce(col("NetworkID"), lit("")).alias("NetworkID")
    )
    
    enriched_security_events_df = enrich_security_events(
        security_events_df, 
        table_dfs["vehicle"], 
        table_dfs["vehicle_model"], 
        table_dfs["fleet_cars"], 
        table_dfs["security_event_lookup"]
    )
    
    insert_security_events_to_elasticsearch(enriched_security_events_df, epoch_id, ELASTICSEARCH_SECURITY_EVENTS_INDEX)
    
    non_empty_ioc = valid_records.filter(size(col("IoC.Parameters")) > 0)
    params_df = non_empty_ioc.select(
        col("ID").alias("ID"),
        col("Alert_ID"),
        col("VinNumber"),
        col("TimeStamp").alias("TimeStamp"),
        explode("IoC.Parameters").alias("Parameter")
    )
    
    flattened_ioc_df = params_df.select(
        col("ID"),
        col("Alert_ID"),
        col("VinNumber"),
        date_format(
            from_unixtime(col("Timestamp").cast("double") / 1000), 
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        ).alias("Timestamp"),
        date_format(
            from_unixtime(col("Parameter.TimeStamp").cast("double") / 1000), 
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        ).alias("ParameterTimestamp"),
        col("Parameter.InternalParameter.ParameterName").alias("ParameterName"),
        col("Parameter.InternalParameter.ParameterType").alias("ParameterType"),
        col("Parameter.InternalParameter.ParameterValue").alias("ParameterValue"),
        col("Parameter.InternalParameter.ParameterUnit").alias("ParameterUnit")
    )
    insert_ioc_parameters_to_elasticsearch(flattened_ioc_df, epoch_id, ELASTICSEARCH_SEVS_LOGS_INDEX)

    time_inserted_to_elasticsearch = datetime.now()
    
    batch_logger(spark, epoch_id)

def reset_counters():
    global total_security_events, total_ioc_parameters, total_lists_received, total_error_records
    total_security_events = 0
    total_ioc_parameters = 0
    total_lists_received = 0
    total_error_records = 0
    logger.info("Reset all counters to 0")

def main():
    global spark
    es = create_es_client()
    create_index_if_not_exists(es, ELASTICSEARCH_SECURITY_EVENTS_INDEX, security_events_mappings)
    create_index_if_not_exists(es, ELASTICSEARCH_SEVS_LOGS_INDEX, sevs_logs_mappings)

    spark = SparkSession.builder \
        .appName("SEVsToElasticsearch") \
        .config("spark.sql.streaming.checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/checkpoint/sevs02/") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    reset_counters()

    query = kafka_df.writeStream \
        .foreachBatch(process_sev_batch) \
        .option("checkpointLocation", "s3://aws-emr-studio-381492251123-eu-central-1/stream_checkpoint/checkpoint/sevs01/") \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()