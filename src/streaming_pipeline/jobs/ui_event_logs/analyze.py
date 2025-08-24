import traceback
import logging
from datetime import datetime
from typing import Tuple, Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, explode, from_unixtime, to_timestamp, date_format,
    to_json, struct, lit, current_timestamp, when, broadcast, window, count
)
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from streaming_pipeline.shared.dal import ElasticsearchHandler, KafkaHandler, PostgresHandler, DataFrameStorageType
from streaming_pipeline.shared.schema.schema_validator import SchemaValidator
from streaming_pipeline.shared.batch_logger import BatchLogger
from streaming_pipeline.shared.schema.schema_utils import process_error_records, get_schema_from_registry, create_error_record
from streaming_pipeline.shared.job_monitor import SparkJobMonitor
from streaming_pipeline.shared.acks_handler import AckHandler
from streaming_pipeline.shared.parallel_insert import insert_event_logs_data_parallel
from configs.config import Config
import time

logger = logging.getLogger(__name__)

def transform_raw_data(df: DataFrame, vehicles_lookup_df: DataFrame, 
                      signals_lookup_df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Transform raw data for event logs with dynamic DTC detection"""
    with monitor as m:
        m.mark_job("Transform Raw Data")
        
        # Explode and transform data
        processed_df = (df
            .select(
                col("message_id"),
                col("VinNumber").alias("VIN"),
                col("Campaign_ID"),
                explode(col("DataEntries")).alias("entry")
            )
            .select(
                col("message_id"),
                col("VIN"),
                col("Campaign_ID"),
                col("entry.DataName").alias("Signal_ID"),  # Original signal ID
                col("entry.DataValue").alias("Signal_Value"),
                col("entry.DataType").alias("Datatype"),
                col("entry.TimeStamp"),
                to_timestamp(col("entry.TimeStamp").cast("double") / 1000).alias("event_time")
            )
            .withColumn(
                "Timestamp",
                date_format(col("event_time"), Config.UI_DATE_FORMAT)
            ))

        m.explain_plan(processed_df, "After Initial Transform")

        # Join with vehicles lookup first
        if vehicles_lookup_df is not None:
            processed_df = processed_df.join(
                broadcast(vehicles_lookup_df),
                processed_df.VIN == vehicles_lookup_df.vin,
                "left"
            ).drop(vehicles_lookup_df.vin)
        else:
            logger.warning("vehicles_lookup_df data unavailable!")

        # Join with signals using original Signal_ID to get signal names
        if signals_lookup_df is not None:
            # Drop signal_id from lookup after join to avoid ambiguity
            processed_df = processed_df.join(
                broadcast(signals_lookup_df),
                processed_df.Signal_ID == signals_lookup_df.signal_id,
                "left"
            ).drop(signals_lookup_df.signal_id)
        else:
            logger.warning("signals_lookup_df data unavailable!")

        # Now apply the DTC-based transformation logic after we have signal_name available
        # Create a condition to check if signal_name contains DTC-related terms
        dtc_condition = col("signal_name").rlike(f"(?i).*({'|'.join(Config.DTC_KEYWORDS)}).*")


        processed_df = processed_df.withColumn(
            "Signal_Value",
            when(
                dtc_condition &  # is a DTC signal
                col("Signal_Value").isin("0", "1"),  # AND value is 0 or 1
                when(col("Signal_Value") == "0", "passed")
                .otherwise("failed")
            ).otherwise(col("Signal_Value"))  # Keep original for non-0/1 values
        )

        # Select final columns without ambiguity
        final_df = processed_df.select(
            col("message_id"),
            col("event_time"),
            col("Timestamp"),
            col("signal_name").alias("Signal_Name"),
            col("Signal_Value"),
            col("signal_unit").alias("Signal_Unit"),
            col("VIN"),
            col("vehicle_model").alias("Vehicle_Model"),
            col("region").alias("Region"),  # Include the region field
            col("fleet_name").alias("Fleet_Name"),
            col("fleet_id").alias("Fleet_ID"),  # Include fleet_id
            col("Campaign_ID"),
            col("Datatype"),
            col("min_value").cast("string").alias("Min"),
            col("max_value").cast("string").alias("Max"),
            col("signal_fully_qualified_name").alias("Signal_Fully_Qualified_Name"),
            col("Signal_ID")
        )
        
        m.explain_plan(final_df, "After DTC-aware Transform")
        
        return final_df

def transform_for_aggregation(df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Transform data for aggregation"""
    with monitor as m:
        m.mark_job("Transform for Aggregation")
        
        optimal_partitions = Config.NUM_EXECUTORS * Config.EXECUTOR_CORES
        
        return df \
            .withColumn("window", window(col("Timestamp"), "1 hour"))

def aggregate_data(df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Perform aggregations"""
    with monitor as m:
        m.mark_job("Aggregate Data")
        
        window_spec = Window.partitionBy(
            "window", "VIN", "Signal_ID", "Signal_Name", "Region", "Fleet_ID"  # Include Region and Fleet_ID in partitioning
        )
        
        agg_df = df.withColumn(
            "count",
            count("*").over(window_spec)
        ).select(
            "window",
            "VIN",
            "Signal_ID",
            "Signal_Name",
            "Region",  
            "Fleet_Name",
            "Fleet_ID",  # Include Fleet_ID in output
            "count"
        ).distinct()

        return agg_df

def format_for_elasticsearch(raw_df: DataFrame, agg_df: DataFrame,
                           es_handler: ElasticsearchHandler,
                           monitor: SparkJobMonitor) -> Tuple[DataFrame, DataFrame]:
    """Format both raw and aggregated data for Elasticsearch"""
    with monitor as m:
        m.mark_job("Format for Elasticsearch")
        
        # Format raw data
        raw_formatted = es_handler.add_log_id(
            raw_df,
            "Log_ID",
            ["Campaign_ID", "message_id", "VIN", "Timestamp", "Signal_ID", "Signal_Value"]
        )
        
        # Format aggregated data
        agg_formatted = es_handler.add_log_id(
            agg_df.select(
                date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
                date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
                "VIN",
                "Signal_ID",
                "Signal_Name",
                "Region",
                col("Fleet_Name").alias("Fleet"),
                col("Fleet_ID").alias("Fleet_ID"),  # Include Fleet_ID
                "count"
            ),
            "es_id",
            ["VIN", "window_start", "Signal_ID", "Signal_Name", "Region"]  # Keep ID generation the same
        )
        
        return raw_formatted, agg_formatted



def insert_data(es_handler: ElasticsearchHandler,
                raw_df: DataFrame,
                agg_df: DataFrame,
                monitor: SparkJobMonitor,
                batch_id: int) -> Optional[str]:
    """Insert data to Elasticsearch with transaction behavior"""
    with monitor as m:        
        inserted_agg = False
        try:
            # Insert raw data first
            m.mark_job("Insert Raw Data to Elasticsearch")
            es_handler.insert_dataframe(
                raw_df,
                Config.ELASTICSEARCH_EVENT_LOGS_INDEX,
                mapping_id="Log_ID"
            )
            
            # Insert aggregations
            m.mark_job("Insert Aggregations to Elasticsearch")
            es_handler.insert_aggregations(agg_df, Config.ELASTICSEARCH_EVENT_LOGS_AGGS_INDEX)
            inserted_agg = True
            
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        except Exception as e:
            logger.error(f"Error during insertion: {str(e)}")
            error_df = create_error_record(
                agg_df if inserted_agg else raw_df,
                f"INSERTION_ERROR_{Config.ELASTICSEARCH_EVENT_LOGS_AGGS_INDEX if inserted_agg else Config.ELASTICSEARCH_EVENT_LOGS_INDEX}",
                str(e)
            )
            if inserted_agg:
                rollback_inserts(es_handler, agg_df, raw_df, monitor, batch_id)
            process_error_records(error_df, batch_id, Config, 'event_logs')
            raise Exception("Insertion failed. Check error records for details.")  # Prevent offset commit

def rollback_inserts(es_handler: ElasticsearchHandler, 
                    agg_df: DataFrame,
                    raw_df: DataFrame,
                    monitor: SparkJobMonitor, 
                    batch_id: int) -> None:
    """Rollback inserts with memory-efficient approach that avoids collect()"""
    with monitor as m:
        m.mark_job("Rollback Operations")
        client = es_handler._create_client()
        rollback_error = False
        
        try:
            # 1. Handle aggregations first
            aggs_index = Config.ELASTICSEARCH_EVENT_LOGS_AGGS_INDEX
            try:
                # Get IDs without collect() - use take(100) which is more lightweight
                agg_ids = []
                for row in agg_df.select("es_id").limit(100).take(100):
                    agg_ids.append(row.es_id)
                
                # Process in batches for better performance
                if agg_ids:
                    for agg_id in agg_ids:
                        try:
                            # Check if record exists
                            existing = client.get(
                                index=aggs_index,
                                id=agg_id,
                                _source=["count"],
                                ignore=[404]
                            )
                            
                            if existing.get('found', False):
                                # Just delete the record - simpler approach
                                client.delete(
                                    index=aggs_index,
                                    id=agg_id,
                                    ignore=[404]
                                )
                        except Exception as e:
                            rollback_error = True
                            logger.error(f"Error rolling back aggregation {agg_id}: {str(e)}")
            except Exception as e:
                rollback_error = True
                logger.error(f"Error handling agg rollback: {e}")

            # 2. Delete raw data
            raw_index = Config.ELASTICSEARCH_EVENT_LOGS_INDEX
            try:
                # Use take() instead of collect()
                raw_ids = []
                for row in raw_df.select("Log_ID").limit(100).take(100):
                    raw_ids.append(row.Log_ID)
                
                if raw_ids:
                    for raw_id in raw_ids:
                        try:
                            client.delete(
                                index=raw_index,
                                id=raw_id,
                                ignore=[404]  # Ignore if already deleted
                            )
                        except Exception as e:
                            rollback_error = True
                            logger.error(f"Error rolling back raw data {raw_id}: {str(e)}")
            except Exception as e:
                rollback_error = True
                logger.error(f"Error handling raw rollback: {e}")
                
        except Exception as e:
            logger.error(f"Error during rollback: {str(e)}")
        finally:
            client.close()
            # Don't raise exception - let the process continue
            if rollback_error:
                logger.error("Rollback encountered errors. Some data may not be completely rolled back.")
                # Log error but don't raise - prevent cascading failures

def process_batch(spark: SparkSession, df: DataFrame, batch_id: int,
                 kafka_handler: KafkaHandler,
                 es_handler: ElasticsearchHandler,
                 batch_logger: BatchLogger,
                 vehicles_lookup_df: DataFrame,
                 signals_lookup_df: DataFrame) -> None:
    """Process each batch with unified transformation and insertion"""
    
    monitor = SparkJobMonitor(spark)
    error_types = set()
    time_received_from_kafka = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    time_inserted_to_elasticsearch = None
    batch_start_time = time.time()

    try:
        if df.isEmpty():
            logger.info(f"Batch {batch_id}: Empty DataFrame")
            return
        
        # Parse and validate
        with monitor as m:
            m.mark_job(f"Batch {batch_id}: Parse Data")
            valid_records, error_df = kafka_handler.parse_without_validation(
                spark, df, "logs_schema"
            )

            if error_df.take(1):
                error_types.add("VALIDATION_ERROR")
                process_error_records(error_df, batch_id, Config, 'event_logs')
                # Raise exception if validation fails
                raise Exception("Validation failed. Check error records for details.")

            if valid_records.take(1):
                # Transform raw data
                raw_data = transform_raw_data(
                    valid_records, vehicles_lookup_df, signals_lookup_df, monitor
                )
                
                # Transform for aggregation
                agg_ready = transform_for_aggregation(raw_data, monitor)
                
                # Perform aggregations
                aggregated = aggregate_data(agg_ready, monitor)
                
                # Format both for Elasticsearch
                raw_formatted, agg_formatted = format_for_elasticsearch(
                    raw_data, aggregated, es_handler, monitor
                )
                
                # Insert with parallel execution
                try:
                    time_inserted_to_elasticsearch = insert_event_logs_data_parallel(
                        es_handler, raw_formatted, agg_formatted, monitor, batch_id, Config
                    )
                except Exception as insert_error:
                    error_types.add("INSERTION_ERROR")
                    logger.error(f"Parallel insert operation failed: {str(insert_error)}")
                    raise  # Re-raise to prevent offset commit
                
        logger.info(f"Batch {batch_id} completed in {time.time() - batch_start_time:.2f}s")
        
    except Exception as e:
        error_types.add("PROCESSING_ERROR")
        logger.error(f"Batch {batch_id} failed: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Log batch status with error before raising
        try:
            if df is not None and not df.isEmpty():
                batch_logger.log_batch(
                    job_type="event_logs",
                    step_id=str(spark.sparkContext.applicationId),
                    batch_number=batch_id,
                    kafka_df=df,
                    kafka_timestamp=time_received_from_kafka,
                    time_received_from_kafka=time_received_from_kafka,
                    time_inserted_to_elasticsearch=time_inserted_to_elasticsearch,
                    is_error=True,
                    error_types=list(error_types)
                )
        except Exception as log_error:
            logger.error(f"Failed to log error status: {str(log_error)}")
        
        # Raise to prevent offset commit
        raise Exception(f"Batch {batch_id} processing failed: {str(e)}")
        
    else:
        # Only log success if no exceptions occurred
        try:
            if df is not None and not df.isEmpty():
                batch_logger.log_batch(
                    job_type="event_logs",
                    step_id=str(spark.sparkContext.applicationId),
                    batch_number=batch_id,
                    kafka_df=df,
                    kafka_timestamp=time_received_from_kafka,
                    time_received_from_kafka=time_received_from_kafka,
                    time_inserted_to_elasticsearch=time_inserted_to_elasticsearch,
                    is_error=bool(error_types),
                    error_types=list(error_types) if error_types else None
                )
        except Exception as log_error:
            logger.error(f"Failed to log success status: {str(log_error)}")
            # Don't raise here as the batch was successful

def analyze(spark: SparkSession):
    """Main analysis function for unified event logs processing"""
    logger.info("Initializing unified event logs processing")
    monitor = SparkJobMonitor(spark)
    query = None

    with PostgresHandler(Config.POSTGRES_URL, Config.POSTGRES_PROPERTIES) as ps_handler:
        try:
            with monitor as m:
                # Initialize handlers
                m.mark_job("Initialize Services")
                es_handler = ElasticsearchHandler(
                    host=Config.ES_HOST,
                    port=Config.ES_PORT,
                    scheme=Config.ES_SCHEME,
                    fixed_namespace_uuid=Config.FIXED_NAMESPACE_UUID,
                    username=getattr(Config, 'ES_USERNAME', None),
                    password=getattr(Config, 'ES_PASSWORD', None)
                )

                batch_logger = BatchLogger(es_handler, spark)
                batch_logger.create_logging_index_if_not_exists('event_logs')

                # Load lookup data
                m.mark_job("Load Lookup Data")
                vehicles_lookup_df = ps_handler.load_by_query(
                    spark,
                    Config.VEHICLES_LOOKUP_QUERY,
                    "vehicles_lookup_df",
                    DataFrameStorageType.CACHED
                )

                signals_lookup_df = ps_handler.load_by_query(
                    spark,
                    Config.SIGNALS_LOOKUP_QUERY,
                    "signals_lookup_df",
                    DataFrameStorageType.CACHED
                )

                if vehicles_lookup_df is None or signals_lookup_df is None:
                    raise ValueError("Failed to load required lookup tables")

                vehicles_lookup_df = broadcast(vehicles_lookup_df)
                signals_lookup_df = broadcast(signals_lookup_df)

                # Initialize Kafka
                m.mark_job("Initialize Kafka")
                schema_validator = SchemaValidator()
                json_schema = get_schema_from_registry(
                    Config.GLUE_SCHEMA_REGISTRY_NAME,
                    Config.GLUE_SCHEMA_REGISTRY_REGION,
                    "logs_schema"
                )
                schema_validator.register_schema_in_validator("logs_schema", json_schema)

                kafka_handler = KafkaHandler(
                    Config.KAFKA_BOOTSTRAP_SERVERS,
                    Config.KAFKA_LOGS_TOPIC,
                    schema_validator,
                    skip_late_handling=True
                )

                # Create and start stream
                kafka_df = kafka_handler.create_kafka_stream(spark)
                query = kafka_df.writeStream \
                    .foreachBatch(lambda df, epoch_id: process_batch(
                        spark, df, epoch_id,
                        kafka_handler, es_handler, batch_logger,
                        vehicles_lookup_df, signals_lookup_df
                    )) \
                    .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_LOGS) \
                    .trigger(processingTime=Config.PROCESSING_TIME) \
                    .start()

                logger.info("Unified event logs streaming started successfully")
                query.awaitTermination()

        except Exception as e:
            logger.error("Unified event logs processing failed", exc_info=True)
            raise

        finally:
            logger.info("Cleaning up resources...")
            if query is not None and query.isActive:
                try:
                    query.stop()
                    logger.info("Streaming query stopped")
                except Exception as e:
                    logger.error(f"Error stopping streaming query: {str(e)}")