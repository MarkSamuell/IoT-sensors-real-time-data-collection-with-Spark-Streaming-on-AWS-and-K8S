import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, date_format, window, broadcast, 
    when, count, window, concat_ws, concat, element_at, expr, lit, current_timestamp
)
from pyspark.sql.window import Window
from streaming_pipeline.shared.dal.elasticsearch_handler import ElasticsearchHandler
from streaming_pipeline.shared.dal.kafka_handler import KafkaHandler
from streaming_pipeline.shared.dal.postgres_handler import PostgresHandler, DataFrameStorageType
from streaming_pipeline.shared.schema.schema_utils import (
    get_schema_from_registry, process_error_records, create_error_record
)
from streaming_pipeline.shared.schema.optimized_schema_validator import OptSchemaValidator
from streaming_pipeline.shared.batch_logger import BatchLogger
from configs.config import Config
import traceback

logger = logging.getLogger(__name__)

def transform_records(df: DataFrame, security_events_lookup_df: DataFrame):
    """
    Transform records with ID='005' handling and sev_name concatenation
    before aggregation
    """
    try:
        # First format timestamp properly for later use
        df = df.withColumn(
            "event_time",
            to_timestamp(col("Timestamp").cast("double") / 1000)
        )

        # Handle array access safely
        df = df.withColumn(
            "first_parameter",
            when(
                col("ID") == '005',
                element_at(col("IoC.Parameters"), 1)
            )
        )

        # Transform ID 
        df = df.withColumn(
            "transformed_id",
            when(
                col("ID") == '005',
                concat_ws(
                    "-",
                    col("ID"),
                    expr("first_parameter.InternalParameter.ParameterValue")
                )
            ).otherwise(col("ID"))
        )

        # Join with security events lookup using original ID
        df = df.join(broadcast(security_events_lookup_df), df.ID == security_events_lookup_df.sev_id, "left")

        # Add sev_name_suffix and handle concatenation
        df = df.withColumn(
            "sev_name_suffix",
            when(
                col("ID") == '005',
                concat(
                    lit("-"),
                    expr("first_parameter.InternalParameter.ParameterType")
                )
            ).otherwise("")
        ).withColumn(
            "final_sev_name",
            when(
                col("ID") == '005',
                concat(col("sev_name"), col("sev_name_suffix"))
            ).otherwise(col("sev_name"))
        )

        # Select relevant columns with transformed values
        transformed_df = df.select(
            col("transformed_id").alias("ID"),
            col("final_sev_name").alias("SEV_Name"),
            col("VinNumber"),
            col("Severity"),
            col("event_time")  # Using pre-formatted timestamp
        )

        transformed_df.printSchema()  # Debug output
        transformed_df.show(5, truncate=False)  # Debug output

        logger.info("Successfully transformed records with ID='005' handling")
        return transformed_df

    except Exception as e:
        logger.error(f"Error transforming records: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_current_events(df: DataFrame, vehicles_lookup_df: DataFrame, 
                         security_events_lookup_df: DataFrame, es_handler: ElasticsearchHandler) -> str:
    """
    Process current (non-late) events and insert into Elasticsearch
    """
    try:
        # Transform records (handle ID='005' and sev_name)
        transformed_records = transform_records(df, security_events_lookup_df)

        # Create windows and aggregate on transformed values
        windowed_records = transformed_records.withColumn(
            "window",
            window(col("event_time"), "1 hour")
        )

        # Define window spec for counting using transformed values
        window_spec = Window.partitionBy(
            "window",
            "ID",  # Using transformed ID
            "SEV_Name",  # Using concatenated sev_name
            "VinNumber",
            "Severity"
        )

        # Aggregate using transformed values
        aggregated_df = windowed_records.withColumn(
            "count",
            count("*").over(window_spec)
        ).select(
            "window",
            "ID",
            "SEV_Name",
            "VinNumber", 
            "Severity",
            "count"
        ).distinct()

        # Debug output
        logger.info(f"Aggregation results before enrichment:")
        aggregated_df.show(5, truncate=False)

        # Join with vehicles lookup after aggregation
        enriched_df = aggregated_df \
            .join(broadcast(vehicles_lookup_df), 
                  aggregated_df.VinNumber == vehicles_lookup_df.vin, 
                  "left") \
            .withColumn(
                "Fleet",
                when(col("fleet_name").isNull(), "Unknown")
                    .otherwise(col("fleet_name"))
            )

        # Format for Elasticsearch with consistent naming
        formatted_df = enriched_df.select(
            date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
            date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
            "ID",
            "SEV_Name",
            "Severity",
            col("VinNumber").alias("VIN"),
            "Fleet",
            "count"
        )

        # Debug output
        logger.info(f"Final formatted data:")
        formatted_df.show(5, truncate=False)

        # Add ID for Elasticsearch with consistent fields
        final_df = es_handler.add_log_id(
            formatted_df,
            "es_id",
            ["VIN", "window_start", "ID", "SEV_Name", "Severity"]
        )

        # Insert into Elasticsearch
        record_count = final_df.count()
        es_handler.insert_aggregations(final_df, Config.ELASTICSEARCH_SECURITY_EVENTS_AGGS_INDEX)
        logger.info(f"Successfully processed and inserted {record_count} aggregated records")
        
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        logger.error(f"Error processing current events: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_batch(spark: SparkSession, df: DataFrame, batch_id: int, kafka_handler: KafkaHandler, 
                ps_handler: PostgresHandler, es_handler: ElasticsearchHandler, 
                vehicles_lookup_df: DataFrame, security_events_lookup_df: DataFrame,
                batch_logger: BatchLogger):
    """Process each batch coming from Kafka using window functions"""

    error_types = set()
    kafka_timestamp = None
    time_received_from_kafka = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    time_inserted_to_elasticsearch = None

    if df.isEmpty():
        logger.info(f"Batch {batch_id}: Received empty DataFrame. Skipping processing.")
        batch_logger.log_batch(
            job_type="sevs_aggs",
            step_id=str(spark.sparkContext.applicationId),
            batch_number=batch_id,
            kafka_df=df,
            kafka_timestamp=kafka_timestamp,
            time_received_from_kafka=time_received_from_kafka,
            time_inserted_to_elasticsearch=None,
            is_error=False,
            error_types=None
        )
        return

    try:
        kafka_timestamp_row = df.select("timestamp").agg({"timestamp": "min"}).collect()[0]
        if kafka_timestamp_row[0] is not None:
            kafka_timestamp = kafka_timestamp_row[0].strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Batch {batch_id}: Processing {df.count()} messages")
        
        # Parse and validate records
        valid_records, error_df = kafka_handler.parse_without_validation(spark, df, "sevs_schema")

        if error_df.take(1):
            error_types.add("VALIDATION_ERROR")
            process_error_records(error_df, batch_id, Config, 'sevs_aggs')

        if valid_records.take(1):
            try:
                time_inserted_to_elasticsearch = process_current_events(
                    valid_records, 
                    vehicles_lookup_df, 
                    security_events_lookup_df,
                    es_handler
                )

            except Exception as e:
                error_types.add("PROCESSING_ERROR")
                error_df = create_error_record(
                    valid_records,
                    "PROCESSING_ERROR",
                    str(e)
                )
                process_error_records(error_df, batch_id, Config, 'sevs_aggs')
                logger.error(f"Error processing batch: {str(e)}")
                logger.error(traceback.format_exc())

    except Exception as e:
        logger.error(f"Batch {batch_id}: Processing error: {str(e)}")
        logger.error(traceback.format_exc())
        error_types.add("UNHANDLED_ERROR")
        time_inserted_to_elasticsearch = None

    finally:
        try:
            batch_logger.log_batch(
                job_type="sevs_aggs",
                step_id=str(spark.sparkContext.applicationId),
                batch_number=batch_id,
                kafka_df=df,
                kafka_timestamp=kafka_timestamp,
                time_received_from_kafka=time_received_from_kafka,
                time_inserted_to_elasticsearch=time_inserted_to_elasticsearch,
                is_error=bool(error_types),
                error_types=list(error_types) if error_types else None
            )
        except Exception as logging_error:
            logger.error(f"Failed to log batch status: {str(logging_error)}")

def analyze(spark: SparkSession):
    """Main analysis function for SEVs aggregations"""
    logger.info("Initializing SEVs Aggregations analysis")
    
    with PostgresHandler(Config.POSTGRES_URL, Config.POSTGRES_PROPERTIES) as ps_handler:
        try:
            # Initialize handlers
            es_handler = ElasticsearchHandler(
                host=Config.ES_HOST,
                port=Config.ES_PORT,
                scheme=Config.ES_SCHEME,
                fixed_namespace_uuid=Config.FIXED_NAMESPACE_UUID,
                username=getattr(Config, 'ES_USERNAME', None),  # Will be None if not in config
                password=getattr(Config, 'ES_PASSWORD', None)   # Will be None if not in config
            )

            # Initialize batch logger
            batch_logger = BatchLogger(es_handler, spark)
            batch_logger.create_logging_index_if_not_exists('sevs_aggs')

            # Load lookup tables
            vehicles_lookup_df = ps_handler.load_by_query(
                spark,
               """
                    SELECT 
                        v.vin,
                        vm.name as vehicle_model,
                        COALESCE(vm.region, 'Unknown') as region,
                        COALESCE(f.name, 'Unknown') as fleet_name
                    FROM vehicle v
                    LEFT JOIN vehicle_model vm ON v.vehicle_model_id = vm.id
                    LEFT JOIN fleet_vehicles_vehicle fvv ON fvv.vehicle_id = v.id
                    LEFT JOIN fleet f ON f.id = fvv.fleet_id
                    WHERE v.is_deleted = false 
                    AND (vm.is_deleted IS NULL OR vm.is_deleted = false)
                    AND (f.is_deleted IS NULL OR f.is_deleted = false)
                    """,
                "vehicles_lookup_df",
                DataFrameStorageType.CACHED
            )

            security_events_lookup_df = ps_handler.load_by_query(
                spark,
                """ SELECT
                    business_id as sev_id, 
                    name as sev_name
                    FROM 
                    security_event
                """,
                "security_events_lookup_df",
                DataFrameStorageType.CACHED
            )

            if vehicles_lookup_df is None or security_events_lookup_df is None:
                raise ValueError("Failed to load required lookup tables")

            # Initialize schema validator
            schema_validator = OptSchemaValidator()
            json_schema = get_schema_from_registry(
                Config.GLUE_SCHEMA_REGISTRY_NAME,
                Config.GLUE_SCHEMA_REGISTRY_REGION,
                "sevs_schema"
            )
            schema_validator.register_schema_in_validator("sevs_schema", json_schema)
            
            kafka_handler = KafkaHandler(
                Config.KAFKA_BOOTSTRAP_SERVERS,
                Config.KAFKA_SEVS_TOPIC,
                schema_validator,
                skip_late_handling=True
            )

            # Create and process stream
            kafka_df = kafka_handler.create_kafka_stream(spark)
            
            query = kafka_df.writeStream \
                .foreachBatch(lambda df, epoch_id: process_batch(
                    spark, df, epoch_id, kafka_handler, ps_handler,
                    es_handler, vehicles_lookup_df, security_events_lookup_df,
                    batch_logger
                )) \
                .trigger(processingTime=Config.PROCESSING_TIME) \
                .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_SEVS_AGGS) \
                .start()

            logger.info("Streaming query started successfully")
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Fatal error in SEVs Aggregations analysis: {str(e)}")
            logger.error(traceback.format_exc())
            if 'query' in locals() and query.isActive:
                query.stop()
            raise