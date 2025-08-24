import traceback
import logging
from datetime import datetime
from typing import Tuple, Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, explode, when, size, from_unixtime, to_timestamp, date_format,
    to_json, struct, lit, current_timestamp, concat_ws, concat, element_at, expr,
    window, count, broadcast
)
from pyspark.sql.types import StringType
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from streaming_pipeline.shared.dal import ElasticsearchHandler, KafkaHandler, PostgresHandler, DataFrameStorageType
from streaming_pipeline.shared.schema.schema_validator import SchemaValidator
from streaming_pipeline.shared.batch_logger import BatchLogger
from streaming_pipeline.shared.job_monitor import SparkJobMonitor
from streaming_pipeline.shared.schema.schema_utils import process_error_records, get_schema_from_registry, create_error_record
from streaming_pipeline.shared.parallel_insert import insert_sevs_data_parallel
from configs.config import Config
import time

logger = logging.getLogger(__name__)

def transform_raw_sevs(df: DataFrame, vehicles_lookup_df: DataFrame, 
                     security_events_lookup_df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Transform raw security events data with dynamic NRC detection"""
    with monitor as m:
        m.mark_job("Transform Raw SEVs")
        
        # First format timestamps properly
        df = df.withColumn(
            "event_time", 
            to_timestamp(col("Timestamp").cast("double") / 1000)
        ).withColumn(
            "Timestamp",
            date_format(col("event_time"), Config.UI_DATE_FORMAT)
        )
        
        # Do the joins first to get the sev_name for checking
        if vehicles_lookup_df is not None:
            df = df.join(broadcast(vehicles_lookup_df), df.VinNumber == vehicles_lookup_df.vin, "left")
            logger.info("Successfully joined with vehicles lookup data")
        else:
            logger.warning("vehicles_lookup_df data unavailable!")

        if security_events_lookup_df is not None:
            df = df.join(broadcast(security_events_lookup_df), df.ID == security_events_lookup_df.sev_id, "left")
            logger.info("Successfully joined with security events lookup data")
        else:
            logger.warning("security_events_lookup_df data unavailable!")

        # Create a condition to check if sev_name contains NRC-related terms
        # This replaces the hardcoded ID == '005' check
        nrc_condition = col("sev_name").rlike(f"(?i).*({'|'.join(Config.NRC_KEYWORDS)}).*")

        # Handle the array access safely for NRC-related events
        df = df.withColumn(
            "first_parameter",
            when(
                nrc_condition,
                element_at(col("IoC.Parameters"), 1)  # element_at uses 1-based indexing
            )
        )

        # Transform the ID and create sev_name_suffix for NRC events
        df = df.withColumn(
            "transformed_id",
            when(
                nrc_condition,
                concat_ws(
                    "-",
                    col("ID"),
                    expr("first_parameter.InternalParameter.ParameterValue")
                )
            ).otherwise(col("ID"))
        ).withColumn(
            "sev_name_suffix",
            when(
                nrc_condition,
                concat(
                    lit("-"),
                    expr("first_parameter.InternalParameter.ParameterType")
                )
            ).otherwise("")
        )

        # Handle sev_name concatenation for NRC events
        df = df.withColumn(
            "sev_name",
            when(
                nrc_condition,
                concat(col("sev_name"), col("sev_name_suffix"))
            ).otherwise(col("sev_name"))
        )

        # Add status column
        df = df.withColumn("SEV_Status", lit("unhandled"))

        # Select final columns with the transformed ID
        final_columns = [
            col("transformed_id").alias("ID"),
            col("IoC"),  # Keep IoC for flattening later
            "VinNumber", 
            "Timestamp",  # This is now properly formatted as yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
            "event_time",
            "SEV_Msg", 
            "Severity", 
            "Origin", 
            "NetworkType", 
            "NetworkID",
            "vehicle_model", 
            "fleet_name",
            "fleet_id",
            col("sev_name").alias("SEV_Name"),
            "rule_id", 
            "description", 
            "SEV_Status",
            col("region").alias("Region")  # Include region for consistency with event logs
        ]

        final_df = df.select(final_columns)
        
        m.explain_plan(final_df, "After Raw SEVs Transform")
        
        return final_df

def create_flattened_ioc_df(df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Create flattened IoC records from raw security events"""
    with monitor as m:
        m.mark_job("Create Flattened IoC")
        
        non_empty_ioc = df.filter(size(col("IoC.Parameters")) > 0)
        params_df = non_empty_ioc.select(
            col("ID"),
            col("Alert_ID"),
            col("VinNumber"),
            col("Timestamp"),
            explode("IoC.Parameters").alias("Parameter")
        )
        
        flattened_df = params_df.select(
            col("ID"),
            col("Alert_ID"),
            col("VinNumber"),
            col("Timestamp"),
            when(col("Parameter.Timestamp").cast("double").isNotNull(),
                 date_format(from_unixtime(col("Parameter.Timestamp").cast("double") / 1000), Config.UI_DATE_FORMAT))
            .alias("ParameterTimestamp"),
            col("Parameter.InternalParameter.ParameterName").alias("ParameterName"),
            col("Parameter.InternalParameter.ParameterType").alias("ParameterType"),
            col("Parameter.InternalParameter.ParameterValue").alias("ParameterValue"),
            col("Parameter.InternalParameter.ParameterUnit").alias("ParameterUnit")
        )
        
        m.explain_plan(flattened_df, "After IoC Flattening")
        
        return flattened_df

def transform_for_aggregation(df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Transform data for aggregation"""
    with monitor as m:
        m.mark_job("Transform for Aggregation")
        
        df_with_window = df.withColumn("window", window(col("Timestamp"), "1 hour"))
        m.explain_plan(df_with_window, "After Adding Window")
        
        return df_with_window

def aggregate_sevs_data(df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Perform aggregations on security events data"""
    with monitor as m:
        m.mark_job("Aggregate SEVs Data")
        
        window_spec = Window.partitionBy(
            "window", "ID", "SEV_Name", "VinNumber", "Severity", "Region", "fleet_id"
        )
        
        agg_df = df.withColumn(
            "count",
            count("*").over(window_spec)
        ).select(
            "window",
            "ID",
            "SEV_Name",
            "VinNumber",
            "Severity", 
            "Region",
            "fleet_name",
            "fleet_id",
            "count"
        ).distinct()
        
        m.explain_plan(agg_df, "After Aggregation")
        
        return agg_df

def format_for_elasticsearch(raw_df: DataFrame, ioc_df: DataFrame, agg_df: DataFrame,
                            es_handler: ElasticsearchHandler,
                            monitor: SparkJobMonitor) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Format raw, IoC, and aggregated data for Elasticsearch"""
    with monitor as m:
        m.mark_job("Format for Elasticsearch")
        
        # 1. Format raw security events
        raw_with_alert_id = es_handler.add_log_id(
            raw_df,
            "Alert_ID",
            ["ID", "VinNumber", "Timestamp", "Severity"]
        )
        
        # Format raw without IoC for insertion
        raw_for_insertion = raw_with_alert_id.drop("IoC", "event_time")
        
        # 2. Format IoC data (if any IoC records exist)
        if ioc_df is not None and ioc_df.count() > 0:
            # Add Alert_ID references to IoC records
            ioc_df_with_alert_id = ioc_df
            
            # Add Log_ID for IoC records
            ioc_for_insertion = es_handler.add_log_id(
                ioc_df_with_alert_id, 
                "Log_ID", 
                ["ID", "Alert_ID", "VinNumber", "Timestamp", "ParameterTimestamp", "ParameterType", "ParameterValue"]
            )
        else:
            ioc_for_insertion = None
        
        # 3. Format aggregated data
        agg_formatted = es_handler.add_log_id(
            agg_df.select(
                date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
                date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
                "ID",
                "SEV_Name",
                "VinNumber",
                "Severity",
                "Region",
                col("fleet_name").alias("Fleet"),
                col("fleet_id").alias("Fleet_ID"),  # Include fleet_id
                "count"
            ),
            "es_id",
            ["VinNumber", "window_start", "ID", "SEV_Name", "Severity"]
        )
        
        return raw_for_insertion, ioc_for_insertion, agg_formatted


def insert_data(es_handler: ElasticsearchHandler,
                raw_df: DataFrame,
                ioc_df: Optional[DataFrame],
                agg_df: DataFrame,
                monitor: SparkJobMonitor,
                batch_id: int) -> Optional[str]:
    """Insert all data types to Elasticsearch with transaction-like behavior"""
    with monitor as m:
        insertion_state = {"raw": False, "ioc": False, "agg": False}
        
        try:
            # 1. Insert raw security events
            m.mark_job("Insert Raw SEVs")
            es_handler.insert_dataframe(
                raw_df,
                Config.ELASTICSEARCH_SECURITY_EVENTS_INDEX,
                mapping_id="Alert_ID"
            )
            insertion_state["raw"] = True
            
            # 2. Insert IoC data if present
            if ioc_df is not None and not ioc_df.isEmpty():
                m.mark_job("Insert IoC Data")
                es_handler.insert_dataframe(
                    ioc_df,
                    Config.ELASTICSEARCH_SEVS_LOGS_INDEX,
                    mapping_id="Log_ID"
                )
                insertion_state["ioc"] = True
            
            # 3. Insert aggregations
            m.mark_job("Insert Aggregations")
            es_handler.insert_aggregations(
                agg_df, 
                Config.ELASTICSEARCH_SECURITY_EVENTS_AGGS_INDEX
            )
            insertion_state["agg"] = True
            
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        except Exception as e:
            logger.error(f"Error during insertion: {str(e)}")
            error_df = create_error_record(
                agg_df if insertion_state["raw"] and insertion_state["ioc"] else
                ioc_df if insertion_state["raw"] else raw_df,
                f"INSERTION_ERROR",
                str(e)
            )
            
            # If any insertion succeeded, try to rollback
            if any(insertion_state.values()):
                rollback_sevs_inserts(es_handler, insertion_state, raw_df, ioc_df, agg_df, monitor, batch_id)
                
            process_error_records(error_df, batch_id, Config, 'sevs')
            raise Exception("Insertion failed. Check error records for details.")

def rollback_sevs_inserts(es_handler, insertion_state, raw_df, ioc_df, agg_df, monitor, batch_id):
    """Safe rollback that works even if SparkContext is shutting down"""
    with monitor as m:
        m.mark_job("Rollback Operations")
        client = es_handler._create_client()
        
        try:
            # 1. Handle aggregations first
            if insertion_state["agg"]:
                aggs_index = Config.ELASTICSEARCH_SECURITY_EVENTS_AGGS_INDEX
                # Direct rollback through ES client without DataFrame operations
                try:
                    agg_ids = []
                    # Get IDs without collect() - use take(100) which is more lightweight
                    for batch in agg_df.select("es_id").limit(100).toLocalIterator():
                        agg_ids.append(batch.es_id)
                    
                    # Delete them directly with ES client
                    if agg_ids:
                        for agg_id in agg_ids:
                            try:
                                client.delete(index=aggs_index, id=agg_id, ignore=[404])
                            except Exception as e:
                                logger.error(f"Error deleting agg record {agg_id}: {e}")
                except Exception as e:
                    logger.error(f"Error handling agg rollback: {e}")
            
            # 2. Handle IoC records
            if insertion_state["ioc"] and ioc_df is not None:
                ioc_index = Config.ELASTICSEARCH_SEVS_LOGS_INDEX
                try:
                    # Use take() instead of collect()
                    ioc_ids = []
                    for row in ioc_df.select("Log_ID").limit(100).take(100):
                        ioc_ids.append(row.Log_ID)
                    
                    if ioc_ids:
                        for ioc_id in ioc_ids:
                            try:
                                client.delete(index=ioc_index, id=ioc_id, ignore=[404])
                            except Exception as e:
                                logger.error(f"Error rolling back IoC {ioc_id}: {e}")
                except Exception as e:
                    logger.error(f"Error handling IoC rollback: {e}")
            
            # 3. Handle raw events
            if insertion_state["raw"]:
                raw_index = Config.ELASTICSEARCH_SECURITY_EVENTS_INDEX
                try:
                    # Use take() instead of collect()
                    alert_ids = []
                    for row in raw_df.select("Alert_ID").limit(100).take(100):
                        alert_ids.append(row.Alert_ID)
                    
                    if alert_ids:
                        for alert_id in alert_ids:
                            try:
                                client.delete(index=raw_index, id=alert_id, ignore=[404])
                            except Exception as e:
                                logger.error(f"Error rolling back raw data {alert_id}: {e}")
                except Exception as e:
                    logger.error(f"Error handling raw rollback: {e}")
                
        except Exception as e:
            logger.error(f"Error during rollback: {str(e)}")
        finally:
            client.close()

def process_batch(spark: SparkSession, df: DataFrame, batch_id: int,
                 kafka_handler: KafkaHandler,
                 es_handler: ElasticsearchHandler,
                 batch_logger: BatchLogger,
                 vehicles_lookup_df: DataFrame,
                 security_events_lookup_df: DataFrame) -> None:
    """Process each batch with unified transformation and insertion for security events"""
    
    monitor = SparkJobMonitor(spark)
    error_types = set()
    time_received_from_kafka = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    time_inserted_to_elasticsearch = None
    batch_start_time = time.time()

    if df.isEmpty():
        logger.info(f"Batch {batch_id}: Empty DataFrame")
        batch_logger.log_batch(
            job_type="sevs",
            step_id=str(spark.sparkContext.applicationId),
            batch_number=batch_id,
            kafka_df=df,
            kafka_timestamp=time_received_from_kafka,  # Use same timestamp instead of collecting
            time_received_from_kafka=time_received_from_kafka,
            time_inserted_to_elasticsearch=None,
            is_error=False,
            error_types=None
        )
        return

    try:
        # Don't use collect() to get Kafka timestamp - use the receipt time
        kafka_timestamp = time_received_from_kafka  # Avoid collect() operation
            
        # Parse and validate
        with monitor as m:
            m.mark_job(f"Batch {batch_id}: Parse Data")
            valid_records, error_df = kafka_handler.parse_without_validation(
                spark, df, "sevs_schema"
            )

            if error_df.take(1):
                error_types.add("VALIDATION_ERROR")
                process_error_records(error_df, batch_id, Config, 'sevs')

            if valid_records.take(1):
                # 1. Transform raw security events
                raw_sevs = transform_raw_sevs(
                    valid_records, vehicles_lookup_df, security_events_lookup_df, monitor
                )
                
                # 2. Create flattened IoC data
                # Need to add Alert_ID before IoC flattening, so do a partial format
                raw_with_alert_id = es_handler.add_log_id(
                    raw_sevs,
                    "Alert_ID",
                    ["ID", "VinNumber", "Timestamp", "Severity"]
                )
                
                ioc_data = create_flattened_ioc_df(raw_with_alert_id, monitor)
                
                # 3. Transform for aggregation
                agg_ready = transform_for_aggregation(raw_sevs, monitor)
                
                # 4. Perform aggregations
                aggregated = aggregate_sevs_data(agg_ready, monitor)
                
                # 5. Format all for Elasticsearch (returns raw without IoC)
                raw_formatted, ioc_formatted, agg_formatted = format_for_elasticsearch(
                    raw_sevs, ioc_data, aggregated, es_handler, monitor
                )
                
                # 6. Insert all data types with parallel execution
                try:
                    time_inserted_to_elasticsearch = insert_sevs_data_parallel(
                        es_handler, raw_formatted, ioc_formatted, agg_formatted, monitor, batch_id, Config
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
        raise
        
    finally:
        try:
            batch_logger.log_batch(
                job_type="sevs",
                step_id=str(spark.sparkContext.applicationId),
                batch_number=batch_id,
                kafka_df=df,
                kafka_timestamp=kafka_timestamp,
                time_received_from_kafka=time_received_from_kafka,
                time_inserted_to_elasticsearch=time_inserted_to_elasticsearch,
                is_error=bool(error_types),
                error_types=list(error_types) if error_types else None
            )
        except Exception as log_error:
            logger.error(f"Failed to log batch status: {str(log_error)}")

def analyze(spark: SparkSession):
    """Main analysis function for unified security events processing"""
    logger.info("Initializing unified security events processing")
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
                batch_logger.create_logging_index_if_not_exists('sevs')

                # Verify required indices exist
                required_indexes = [
                    Config.ELASTICSEARCH_SECURITY_EVENTS_INDEX, 
                    Config.ELASTICSEARCH_SEVS_LOGS_INDEX,
                    Config.ELASTICSEARCH_SECURITY_EVENTS_AGGS_INDEX
                ]
                missing_indexes = [index for index in required_indexes if not es_handler.index_exists(index)]
                
                if missing_indexes:
                    raise ValueError(f"Required Elasticsearch indexes do not exist: {', '.join(missing_indexes)}")

                # Load lookup data
                m.mark_job("Load Lookup Data")
                vehicles_lookup_df = ps_handler.load_by_query(
                    spark,
                    Config.VEHICLES_LOOKUP_QUERY,
                    "vehicles_lookup_df",
                    DataFrameStorageType.CACHED
                )

                security_events_lookup_df = ps_handler.load_by_query(
                    spark,
                    Config.SECURITY_EVENTS_LOOKUP_QUERY,
                    "security_events_lookup_df",
                    DataFrameStorageType.CACHED
                )

                if vehicles_lookup_df is None or security_events_lookup_df is None:
                    raise ValueError("Failed to load required lookup tables")

                # Initialize schema validator
                schema_validator = SchemaValidator()
                json_schema = get_schema_from_registry(
                    Config.GLUE_SCHEMA_REGISTRY_NAME,
                    Config.GLUE_SCHEMA_REGISTRY_REGION,
                    "sevs_schema"
                )
                schema_validator.register_schema_in_validator("sevs_schema", json_schema)

                # Initialize Kafka
                m.mark_job("Initialize Kafka")
                kafka_handler = KafkaHandler(
                    Config.KAFKA_BOOTSTRAP_SERVERS,
                    Config.KAFKA_SEVS_TOPIC,
                    schema_validator,
                    skip_late_handling=True
                )

                # Create and start stream
                kafka_df = kafka_handler.create_kafka_stream(spark)
                
                # Configure checkpoint location for unified sevs
                checkpoint_location = Config.S3_CHECKPOINT_LOCATION_SEVS
                
                query = kafka_df.writeStream \
                    .foreachBatch(lambda df, epoch_id: process_batch(
                        spark, df, epoch_id,
                        kafka_handler, es_handler, batch_logger,
                        vehicles_lookup_df, security_events_lookup_df
                    )) \
                    .option("checkpointLocation", checkpoint_location) \
                    .trigger(processingTime=Config.PROCESSING_TIME) \
                    .start()

                logger.info("Unified security events streaming started successfully")
                query.awaitTermination()

        except Exception as e:
            logger.error("Unified security events processing failed", exc_info=True)
            raise

        finally:
            logger.info("Cleaning up resources...")
            if query is not None and query.isActive:
                try:
                    query.stop()
                    logger.info("Streaming query stopped")
                except Exception as e:
                    logger.error(f"Error stopping streaming query: {str(e)}")