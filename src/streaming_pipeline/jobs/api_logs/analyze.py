import traceback
import logging
from datetime import datetime
from typing import Tuple, Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, explode, to_timestamp, date_format, size,
    to_json, struct, lit, current_timestamp, when, broadcast, window, count, concat_ws, get_json_object, coalesce,
    concat, monotonically_increasing_id, isnan, isnull, length, trim
)
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from streaming_pipeline.shared.dal import ElasticsearchHandler, KafkaHandler, PostgresHandler, DataFrameStorageType
from streaming_pipeline.shared.schema.schema_validator import SchemaValidator
from streaming_pipeline.shared.batch_logger import BatchLogger
from streaming_pipeline.shared.schema.schema_utils import process_error_records, get_schema_from_registry, create_error_record
from streaming_pipeline.shared.parallel_insert import insert_api_logs_data_parallel
from streaming_pipeline.shared.job_monitor import SparkJobMonitor
from streaming_pipeline.shared.acks_handler import AckHandler
from configs.config import Config
import time

logger = logging.getLogger(__name__)

def field_exists_in_schema(schema, field_path):
    """Check if a field path exists in the schema"""
    try:
        # Remove backticks and split the path
        path_parts = field_path.replace("`", "").split(".")
        current_schema = schema
        
        for part in path_parts:
            if hasattr(current_schema, 'fields'):
                # Find the field in the current schema level
                field_found = False
                for field in current_schema.fields:
                    if field.name == part:
                        current_schema = field.dataType
                        field_found = True
                        break
                if not field_found:
                    return False
            else:
                return False
        return True
    except Exception:
        return False

def safe_col_access(field_path: str, schema, json_convert=False):
    """Safely access a column, return None if field doesn't exist in schema"""
    if field_exists_in_schema(schema, field_path):
        if json_convert:
            # For JSON conversion, only convert if field is not null
            return when(
                col(field_path).isNotNull(),
                to_json(col(field_path))
            ).otherwise(lit(None))
        else:
            return when(col(field_path).isNotNull(), col(field_path)).otherwise(lit(None))
    else:
        # Field doesn't exist in schema, return null
        return lit(None)

def validate_json_structure(df: DataFrame) -> DataFrame:
    """Validate JSON structure and add validation flags"""
    try:
        # Add validation flags for raw_kafka_value JSON structure
        validated_df = df.withColumn(
            "json_valid",
            when(
                col("raw_kafka_value").isNull() | 
                (length(trim(col("raw_kafka_value"))) == 0),
                lit(False)
            ).otherwise(lit(True))
        ).withColumn(
            "has_request_data",
            col("`Request Data`").isNotNull()
        ).withColumn(
            "has_response_data", 
            col("`Response Data`").isNotNull()
        )
        
        return validated_df
        
    except Exception as e:
        logger.error(f"JSON validation failed: {str(e)}")
        # Return original dataframe with default validation flags
        return df.withColumn("json_valid", lit(True)) \
                 .withColumn("has_request_data", col("`Request Data`").isNotNull()) \
                 .withColumn("has_response_data", col("`Response Data`").isNotNull())

def transform_raw_api_logs(df: DataFrame, monitor: SparkJobMonitor, es_handler: ElasticsearchHandler) -> DataFrame:
    """Transform raw API logs data with fully dynamic field handling and better error handling"""
    with monitor as m:
        m.mark_job("Transform Raw API Logs")
        
        # Validate JSON structure first
        df = validate_json_structure(df)
        
        # Enhanced logging with offset information for debugging specific failures
        try:
            sample_records = df.select(
                "message_id", "ID", "Request Data", "Response Data", "sevs", 
                "kafka_offset", "json_valid", "has_request_data", "has_response_data"
            ).limit(5).collect()
            
            if sample_records:
                logger.info(f"🔍 Sample incoming records ({len(sample_records)} records):")
                for i, record in enumerate(sample_records):
                    offset_info = f"offset={getattr(record, 'kafka_offset', 'unknown')}"
                    logger.info(f"  Record {i+1}: message_id={record.message_id}, ID={record.ID}, {offset_info}")
                    logger.info(f"    JSON Valid: {record.json_valid}, Request: {'Yes' if record.has_request_data else 'No'}, Response: {'Yes' if record.has_response_data else 'No'}")
                    logger.info(f"    SEVs: {'Yes' if record.sevs is not None else 'No'}")
                    
                    # Special logging for potentially problematic records
                    if not record.json_valid:
                        logger.warning(f"    ⚠️  Record {i+1} has invalid JSON structure")
                        
        except Exception as e:
            logger.warning(f"Could not sample incoming records: {str(e)}")
        
        # Filter out test messages - minimal filtering for performance
        # Handle null message_id (from null Kafka keys) - don't filter them out
        filtered_df = df.filter(
            ((col("message_id") != "test-key") | col("message_id").isNull()) &
            col("ID").isNotNull()
        )
        
        # Check if filtering removed all records (lightweight)
        has_filtered_records = filtered_df.take(1)
        if not has_filtered_records:
            logger.error(f"🚨 All records filtered out! Check message structure")
            return df.filter(lit(False))  # Return empty DataFrame
        
        # Format timestamps properly using unified timestamp parser
        api_logs_df = es_handler.parse_timestamp(
            filtered_df, 
            "Timestamp", 
            "event_time", 
            "Timestamp", 
            Config.UI_DATE_FORMAT
        )
        
        # Get the schema for safe field access
        df_schema = api_logs_df.schema
        
        # Create proper nested structure for Elasticsearch with enhanced error handling
        try:
            # Log records that might cause issues
            invalid_json_count = df.filter(col("json_valid") == False).count() if "json_valid" in df.columns else 0
            if invalid_json_count > 0:
                logger.warning(f"⚠️  Found {invalid_json_count} records with invalid JSON structure")
                
                # Log sample of invalid records for debugging
                try:
                    invalid_samples = df.filter(col("json_valid") == False) \
                                       .select("message_id", "ID", "kafka_offset") \
                                       .limit(3).collect()
                    for sample in invalid_samples:
                        offset_info = f"offset={getattr(sample, 'kafka_offset', 'unknown')}"
                        logger.warning(f"    Invalid JSON record: ID={sample.ID}, message_id={sample.message_id}, {offset_info}")
                except Exception as log_e:
                    logger.warning(f"Could not log invalid JSON samples: {str(log_e)}")
            # Create the request nested object with enhanced error handling for dynamic field extraction
            request_struct = struct(
                safe_col_access("`Request Data`.`Source IP`", df_schema).alias("source_ip"),
                safe_col_access("`Request Data`.`Method`", df_schema).alias("method"),
                safe_col_access("`Request Data`.`Path`", df_schema).alias("path"),
                safe_col_access("`Request Data`.`Campaign ID`", df_schema).alias("campaign_id"),
                safe_col_access("`Request Data`.`Service Port`", df_schema).alias("service_port"),
                safe_col_access("`Request Data`.`Agent ID`", df_schema).alias("agent_id"),
                # Extract Headers with better error handling - only from valid JSON
                when(
                    col("raw_kafka_value").isNotNull() & 
                    col("json_valid") & 
                    (length(trim(col("raw_kafka_value"))) > 0),
                    get_json_object(col("raw_kafka_value"), "$['Request Data']['Headers']")
                ).otherwise(lit(None)).alias("headers"),
                # Extract body with enhanced validation - only if requested_details and Body exist
                when(
                    col("raw_kafka_value").isNotNull() & 
                    col("json_valid") & 
                    (get_json_object(col("raw_kafka_value"), "$['Request Data']['requested_details']").isNotNull()) &
                    (get_json_object(col("raw_kafka_value"), "$['Request Data']['requested_details']['Body']").isNotNull()),
                    get_json_object(col("raw_kafka_value"), "$['Request Data']['requested_details']['Body']")
                ).when(
                    safe_col_access("`Request Data`.`requested_details`.`Body`", df_schema).isNotNull(),
                    safe_col_access("`Request Data`.`requested_details`.`Body`", df_schema, json_convert=True)
                ).otherwise(lit(None)).alias("body"),
                # Extract query_params with enhanced validation - only if requested_details and Query Params exist  
                when(
                    col("raw_kafka_value").isNotNull() & 
                    col("json_valid") & 
                    (get_json_object(col("raw_kafka_value"), "$['Request Data']['requested_details']").isNotNull()) &
                    (get_json_object(col("raw_kafka_value"), "$['Request Data']['requested_details']['Query Params']").isNotNull()),
                    get_json_object(col("raw_kafka_value"), "$['Request Data']['requested_details']['Query Params']")
                ).when(
                    safe_col_access("`Request Data`.`requested_details`.`Query Params`", df_schema).isNotNull(),
                    safe_col_access("`Request Data`.`requested_details`.`Query Params`", df_schema, json_convert=True)
                ).otherwise(lit(None)).alias("query_params")
            ).alias("request")
            
            # Create the response nested object with enhanced error handling for both nested and flat structures
            response_struct = struct(
                # Try nested structure first, then flat structure with better error handling
                when(
                    safe_col_access("`Response Data`.`Response Data`.`status_code`", df_schema).isNotNull(),
                    safe_col_access("`Response Data`.`Response Data`.`status_code`", df_schema)
                ).otherwise(
                    safe_col_access("`Response Data`.`status_code`", df_schema)
                ).alias("status_code"),
                # Extract response headers with enhanced validation - try both nested and flat structures
                when(
                    col("raw_kafka_value").isNotNull() & 
                    col("json_valid") & 
                    (length(trim(col("raw_kafka_value"))) > 0),
                    # Try nested structure first with error handling
                    when(
                        get_json_object(col("raw_kafka_value"), "$['Response Data']['Response Data']['headers']").isNotNull(),
                        get_json_object(col("raw_kafka_value"), "$['Response Data']['Response Data']['headers']")
                    ).otherwise(
                        # Fallback to flat structure with error handling
                        when(
                            get_json_object(col("raw_kafka_value"), "$['Response Data']['headers']").isNotNull(),
                            get_json_object(col("raw_kafka_value"), "$['Response Data']['headers']")
                        ).otherwise(lit(None))
                    )
                ).otherwise(lit(None)).alias("headers")
            ).alias("response")
            
            # Infer message type from message structure (message_type field not included in real messages)
            message_type_inferred = when(
                # If has both Request Data and Response Data, treat as response (complete message)
                safe_col_access("`Request Data`", df_schema).isNotNull() & 
                safe_col_access("`Response Data`", df_schema).isNotNull(),
                lit("response")
            ).when(
                # If only has Request Data, treat as request
                safe_col_access("`Request Data`", df_schema).isNotNull(),
                lit("request")
            ).when(
                # If only has Response Data, treat as response  
                safe_col_access("`Response Data`", df_schema).isNotNull(),
                lit("response")
            ).otherwise(lit("unknown"))
            
            # Base fields for all messages
            base_expressions = [
                col("ID").alias("request_id"),
                col("Timestamp").alias("timestamp"),
                col("event_time"),
                message_type_inferred.alias("message_type"),
                current_timestamp().alias("ingestion_time"),
                col("ID").cast("string").alias("Log_ID"),
                col("kafka_offset")  # Include offset for debugging
            ]
            
            # Conditionally add fields based on inferred message type
            select_expressions = base_expressions.copy()
            
            # Populate request struct for request messages OR messages with both request and response
            select_expressions.append(
                when(
                    (message_type_inferred == "request") | (message_type_inferred == "response"),
                    request_struct
                ).alias("request")
            )
            
            # Populate response struct for response messages (including messages with both)
            select_expressions.append(
                when(message_type_inferred == "response", response_struct).alias("response")
            )
            
            # Add has_response field for response messages (set to true)
            select_expressions.append(
                when(message_type_inferred == "response", lit(True)).alias("has_response")
            )
            
            # Include sevs field for SEV processing (if present)
            select_expressions.append(col("sevs"))
            
            # Apply all transformations at once
            api_logs_df = api_logs_df.select(*select_expressions)
            
            logger.info("Successfully applied nested structure transformations")
            return api_logs_df
            
        except Exception as e:
            logger.error(f"CRITICAL ERROR in API logs transformation: {str(e)}")
            logger.error(f"DataFrame schema: {df.schema}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Make error very visible for debugging
            error_msg = f"API logs transformation failed: {str(e)}"
            logger.error("=" * 100)
            logger.error(f"🚨 TRANSFORMATION ERROR: {error_msg}")
            logger.error("=" * 100)
            
            # Re-raise the exception to make it noticeable and stop processing
            raise Exception(f"API logs transformation failed - check logs for details: {str(e)}")

def transform_api_sevs(api_logs_df: DataFrame, sevs_lookup_df: DataFrame, monitor: SparkJobMonitor, es_handler: ElasticsearchHandler) -> DataFrame:
    """Extract and transform SEVs from API logs (following ui_sevs pattern)"""
    with monitor as m:
        m.mark_job("Transform API SEVs")
        
        # Apply unified timestamp parsing first
        api_logs_df = es_handler.parse_timestamp(
            api_logs_df, 
            "Timestamp", 
            "event_time", 
            "timestamp", 
            Config.UI_DATE_FORMAT
        )
        
        # Infer message type from message structure (same logic as main transform)
        message_type_inferred = when(
            # If has both Request Data and Response Data, treat as response
            col("`Request Data`").isNotNull() & col("`Response Data`").isNotNull(),
            lit("response")
        ).when(
            # If only has Request Data, treat as request
            col("`Request Data`").isNotNull(),
            lit("request")
        ).when(
            # If only has Response Data, treat as response  
            col("`Response Data`").isNotNull(),
            lit("response")
        ).otherwise(lit("unknown"))
        
        # Filter records that have SEVs - both request and response messages can have SEVs
        records_with_sevs = api_logs_df.filter(
            col("sevs").isNotNull() & 
            (size(col("sevs")) > 0)
        )
        
        if records_with_sevs.isEmpty():
            logger.info("No records with SEVs found")
            return api_logs_df.filter(lit(False))
        
        # Explode SEVs array to create individual SEV records - handle both request and response messages
        exploded_sevs = records_with_sevs.select(
            col("ID").alias("request_id"),
            # For source_ip: try to get from Request Data if available (request messages), otherwise use placeholder
            when(
                safe_col_access("`Request Data`.`Source IP`", api_logs_df.schema).isNotNull(),
                safe_col_access("`Request Data`.`Source IP`", api_logs_df.schema)
            ).otherwise(lit("unknown")).alias("source_ip"),
            # For campaign_id and path: only available in request messages
            safe_col_access("`Request Data`.`Campaign ID`", api_logs_df.schema).alias("campaign_id"),
            safe_col_access("`Request Data`.`Path`", api_logs_df.schema).alias("path"),
            # Use unified timestamp parser for consistency
            col("event_time"),  # Now available after timestamp parsing
            col("timestamp"),  # Now properly formatted
            explode(col("sevs")).alias("sev_id"),
            current_timestamp().alias("ingestion_time")
        )
        
        # Join with SEV lookup table (following ui_sevs pattern)
        if sevs_lookup_df is not None:
            sevs_df = exploded_sevs.join(
                broadcast(sevs_lookup_df), 
                exploded_sevs.sev_id == sevs_lookup_df.sev_id, 
                "left"
            ).drop(sevs_lookup_df.sev_id)  # Drop the lookup table's sev_id to avoid ambiguity
            logger.info("Successfully joined with SEVs lookup data")
        else:
            logger.warning("sevs_lookup_df data unavailable!")
            sevs_df = exploded_sevs
        
        # Select final SEV columns
        sevs_df = sevs_df.select(
            col("sev_id"),
            col("sev_name"),
            col("description"),
            col("severity"),
            col("timestamp"),
            col("source_ip")
            # Alert_ID will be generated automatically by add_log_id() function
        )
        
        return sevs_df

def transform_for_aggregation(df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Transform data for aggregation (following ui_event_logs pattern)"""
    with monitor as m:
        m.mark_job("Transform for Aggregation")
        
        # Convert timestamp string to timestamp type for windowing
        return df.withColumn("window", window(to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), "1 hour"))

def aggregate_api_logs(df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Perform aggregations on API logs data (following ui_event_logs pattern)"""
    with monitor as m:
        m.mark_job("Aggregate API Logs Data")
        
        # Aggregate by window, source_ip, campaign_id - handle null request fields from response messages
        agg_df = df.groupBy(
            col("window"),
            coalesce(col("request.source_ip"), lit("unknown")).alias("source_ip"), 
            coalesce(col("request.campaign_id"), lit("unknown")).alias("campaign_id")
        ).agg(
            count("*").alias("count")
        )
        
        return agg_df

def aggregate_api_sevs(df: DataFrame, monitor: SparkJobMonitor) -> DataFrame:
    """Perform aggregations on API SEVs data (following ui_sevs pattern)"""
    with monitor as m:
        m.mark_job("Aggregate API SEVs Data")
        
        # Aggregate by window, sev_id, sev_name, severity, source_ip
        agg_df = df.groupBy(
            "window",
            "sev_id",
            "sev_name", 
            "severity",
            "source_ip"
        ).agg(
            count("*").alias("count")
        )
        
        return agg_df

def format_api_logs_for_elasticsearch(raw_df: DataFrame, agg_df: DataFrame,
                                    es_handler: ElasticsearchHandler,
                                    monitor: SparkJobMonitor) -> Tuple[DataFrame, DataFrame]:
    """Format API logs data for Elasticsearch (following ui_event_logs pattern)"""
    with monitor as m:
        m.mark_job("Format API Logs for Elasticsearch")
        
        # Format raw API logs - use request_id only so request/response pairs share same document ID
        raw_formatted = es_handler.add_log_id(
            raw_df,
            "Log_ID",
            ["request_id"]
        )
        
        # Format aggregated API logs
        agg_formatted = es_handler.add_log_id(
            agg_df.select(
                date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
                date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
                col("source_ip"),
                col("campaign_id"),
                col("count")
            ),
            "es_id",
            ["source_ip", "campaign_id", "window_start"]
        )
        
        return raw_formatted, agg_formatted

def format_api_sevs_for_elasticsearch(raw_df: DataFrame, agg_df: DataFrame,
                                    es_handler: ElasticsearchHandler,
                                    monitor: SparkJobMonitor) -> Tuple[DataFrame, DataFrame]:
    """Format API SEVs data for Elasticsearch (following ui_sevs pattern)"""
    with monitor as m:
        m.mark_job("Format API SEVs for Elasticsearch")
        
        # Format raw API SEVs
        raw_formatted = es_handler.add_log_id(
            raw_df,
            "Alert_ID",
            ["sev_id", "source_ip", "timestamp", "sev_name"]
        )
        
        # Format aggregated API SEVs
        agg_formatted = es_handler.add_log_id(
            agg_df.select(
                date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
                date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
                col("sev_id"),
                col("sev_name"),
                col("severity"),
                col("source_ip"),
                col("count")
            ),
            "es_id",
            ["sev_id", "sev_name", "severity", "source_ip", "window_start"]
        )
        
        return raw_formatted, agg_formatted

def insert_api_logs_data(es_handler: ElasticsearchHandler,
                        raw_df: DataFrame,
                        agg_df: DataFrame,
                        monitor: SparkJobMonitor,
                        batch_id: int) -> Optional[str]:
    """Insert API logs data to Elasticsearch with request/response upsert logic"""
    with monitor as m:
        inserted_agg = False
        try:
            # Insert raw data with upsert functionality (Log_ID = request_id)
            m.mark_job("Upsert API Logs to Elasticsearch")
            
            # Use custom write configuration for upsert operation
            es_write_conf = es_handler.get_es_write_conf(
                index_name=Config.ELASTICSEARCH_API_LOGS_INDEX,
                mapping_id="Log_ID",
                operation="upsert"  # Use upsert to merge request/response data
            )
            
            raw_df.write \
                .format("org.elasticsearch.spark.sql") \
                .options(**es_write_conf) \
                .mode("append") \
                .save(Config.ELASTICSEARCH_API_LOGS_INDEX)
            
            # Insert aggregations
            m.mark_job("Insert API Logs Aggregations to Elasticsearch")
            es_handler.insert_aggregations(agg_df, Config.ELASTICSEARCH_API_LOGS_AGGS_INDEX)
            inserted_agg = True
            
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        except Exception as e:
            logger.error(f"Error during API logs insertion: {str(e)}")
            error_df = create_error_record(
                agg_df if inserted_agg else raw_df,
                f"INSERTION_ERROR_{Config.ELASTICSEARCH_API_LOGS_AGGS_INDEX if inserted_agg else Config.ELASTICSEARCH_API_LOGS_INDEX}",
                str(e)
            )
            if inserted_agg:
                rollback_api_logs_inserts(es_handler, agg_df, raw_df, monitor, batch_id)
            process_error_records(error_df, batch_id, Config, 'api_logs')
            raise Exception("API logs insertion failed. Check error records for details.")

def insert_api_sevs_data(es_handler: ElasticsearchHandler,
                        raw_df: DataFrame,
                        agg_df: DataFrame,
                        monitor: SparkJobMonitor,
                        batch_id: int) -> Optional[str]:
    """Insert API SEVs data to Elasticsearch (following ui_sevs pattern)"""
    with monitor as m:
        inserted_agg = False
        try:
            # Insert raw SEVs data first
            m.mark_job("Insert Raw API SEVs to Elasticsearch")
            es_handler.insert_dataframe(
                raw_df,
                Config.ELASTICSEARCH_API_SEVS_INDEX,
                mapping_id="Alert_ID",
            )
            
            # Insert SEVs aggregations
            m.mark_job("Insert API SEVs Aggregations to Elasticsearch")
            es_handler.insert_aggregations(agg_df, Config.ELASTICSEARCH_API_SEVS_AGGS_INDEX)
            inserted_agg = True
            
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        except Exception as e:
            logger.error(f"Error during API SEVs insertion: {str(e)}")
            error_df = create_error_record(
                agg_df if inserted_agg else raw_df,
                f"INSERTION_ERROR_{Config.ELASTICSEARCH_API_SEVS_AGGS_INDEX if inserted_agg else Config.ELASTICSEARCH_API_SEVS_INDEX}",
                str(e)
            )
            if inserted_agg:
                rollback_api_sevs_inserts(es_handler, agg_df, raw_df, monitor, batch_id)
            process_error_records(error_df, batch_id, Config, 'api_logs')
            raise Exception("API SEVs insertion failed. Check error records for details.")

def rollback_api_logs_inserts(es_handler: ElasticsearchHandler,
                             agg_df: DataFrame,
                             raw_df: DataFrame,
                             monitor: SparkJobMonitor,
                             batch_id: int) -> None:
    """Rollback API logs inserts (following ui_event_logs pattern)"""
    with monitor as m:
        m.mark_job("Rollback API Logs Operations")
        client = es_handler._create_client()
        rollback_error = False
        
        try:
            # Handle aggregations first
            aggs_index = Config.ELASTICSEARCH_API_LOGS_AGGS_INDEX
            try:
                agg_ids = []
                for row in agg_df.select("es_id").limit(100).take(100):
                    agg_ids.append(row.es_id)
                
                if agg_ids:
                    for agg_id in agg_ids:
                        try:
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

            # Delete raw data
            raw_index = Config.ELASTICSEARCH_API_LOGS_INDEX
            try:
                raw_ids = []
                for row in raw_df.select("Log_ID").limit(100).take(100):
                    raw_ids.append(row.Log_ID)
                
                if raw_ids:
                    for raw_id in raw_ids:
                        try:
                            client.delete(
                                index=raw_index,
                                id=raw_id,
                                ignore=[404]
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
            if rollback_error:
                logger.error("Rollback encountered errors. Some data may not be completely rolled back.")

def rollback_api_sevs_inserts(es_handler: ElasticsearchHandler,
                             agg_df: DataFrame,
                             raw_df: DataFrame,
                             monitor: SparkJobMonitor,
                             batch_id: int) -> None:
    """Rollback API SEVs inserts (following ui_sevs pattern)"""
    with monitor as m:
        m.mark_job("Rollback API SEVs Operations")
        client = es_handler._create_client()
        rollback_error = False
        
        try:
            # Handle aggregations first
            aggs_index = Config.ELASTICSEARCH_API_SEVS_AGGS_INDEX
            try:
                agg_ids = []
                for row in agg_df.select("es_id").limit(100).take(100):
                    agg_ids.append(row.es_id)
                
                if agg_ids:
                    for agg_id in agg_ids:
                        try:
                            client.delete(
                                index=aggs_index,
                                id=agg_id,
                                ignore=[404]
                            )
                        except Exception as e:
                            rollback_error = True
                            logger.error(f"Error rolling back SEV aggregation {agg_id}: {str(e)}")
            except Exception as e:
                rollback_error = True
                logger.error(f"Error handling agg rollback: {e}")

            # Delete raw SEVs
            raw_index = Config.ELASTICSEARCH_API_SEVS_INDEX
            try:
                raw_ids = []
                for row in raw_df.select("Alert_ID").limit(100).take(100):
                    raw_ids.append(row.Alert_ID)
                
                if raw_ids:
                    for raw_id in raw_ids:
                        try:
                            client.delete(
                                index=raw_index,
                                id=raw_id,
                                ignore=[404]
                            )
                        except Exception as e:
                            rollback_error = True
                            logger.error(f"Error rolling back raw SEV {raw_id}: {str(e)}")
            except Exception as e:
                rollback_error = True
                logger.error(f"Error handling raw SEVs rollback: {e}")
                
        except Exception as e:
            logger.error(f"Error during rollback: {str(e)}")
        finally:
            client.close()
            if rollback_error:
                logger.error("Rollback encountered errors. Some data may not be completely rolled back.")


def process_batch(spark: SparkSession, df: DataFrame, batch_id: int,
                 kafka_handler: KafkaHandler,
                 es_handler: ElasticsearchHandler,
                 batch_logger: BatchLogger,
                 sevs_lookup_df: DataFrame) -> None:
    """Process each batch with API logs 5-step workflow (following ui_event_logs pattern)"""
    
    monitor = SparkJobMonitor(spark)
    error_types = set()
    time_received_from_kafka = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    time_inserted_to_elasticsearch = None
    batch_start_time = time.time()

    if df.isEmpty():
        logger.info(f"Batch {batch_id}: Empty DataFrame")
        batch_logger.log_batch(
            job_type="api_logs",
            step_id=str(spark.sparkContext.applicationId),
            batch_number=batch_id,
            kafka_df=df,
            kafka_timestamp=time_received_from_kafka,
            time_received_from_kafka=time_received_from_kafka,
            time_inserted_to_elasticsearch=None,
            is_error=False,
            error_types=None
        )
        return

    try:
        kafka_timestamp = time_received_from_kafka
            
        # Parse and validate, keeping raw Kafka value for dynamic field extraction
        with monitor as m:
            m.mark_job(f"Batch {batch_id}: Parse Data")
            
            valid_records, error_df = kafka_handler.parse_without_validation(
                spark, df, "api_logs_schema"
            )
            
            # Enhanced logging with offset tracking for debugging specific failures
            has_valid_records = valid_records.take(1)
            has_error_records = error_df.take(1)
            
            logger.info(f"📊 Batch {batch_id} Parsing Results:")
            logger.info(f"  ✅ Valid records: {'Yes' if has_valid_records else 'No'}")
            logger.info(f"  ❌ Error records: {'Yes' if has_error_records else 'No'}")
            
            # Log offset information for debugging specific failing records
            if has_valid_records:
                try:
                    # Get offset range for this batch
                    offset_info = valid_records.select("kafka_offset").agg(
                        {"kafka_offset": "min", "kafka_offset": "max"}
                    ).collect()[0]
                    min_offset = offset_info["min(kafka_offset)"]
                    max_offset = offset_info["max(kafka_offset)"]
                    logger.info(f"  🔢 Processing offsets: {min_offset} to {max_offset}")
                    
                    # Special attention to problematic offset range (468-469)
                    problematic_records = valid_records.filter(
                        col("kafka_offset").isin([468, 469])
                    )
                    problem_count = problematic_records.count()
                    if problem_count > 0:
                        logger.warning(f"  ⚠️  Found {problem_count} records in problematic offset range (468-469)")
                        
                        # Log detailed structure of problematic records
                        try:
                            problem_samples = problematic_records.select(
                                "kafka_offset", "message_id", "ID", 
                                "has_request_data", "has_response_data", "json_valid"
                            ).collect()
                            
                            for sample in problem_samples:
                                logger.warning(f"    🔍 Offset {sample.kafka_offset}: ID={sample.ID}, "
                                             f"Request={'Yes' if sample.has_request_data else 'No'}, "
                                             f"Response={'Yes' if sample.has_response_data else 'No'}, "
                                             f"JSON_Valid={sample.json_valid}")
                        except Exception as debug_e:
                            logger.warning(f"    Could not log problematic record details: {str(debug_e)}")
                            
                except Exception as offset_e:
                    logger.warning(f"  Could not extract offset information: {str(offset_e)}")
            
            # Handle null message_id for messages without Kafka keys BEFORE transformation
            if has_valid_records:
                logger.info(f"🔑 Processing valid records and handling null Kafka keys")
                
                # Generate unique message_id for null keys (no join needed - raw_kafka_value is already included)
                valid_records = valid_records.withColumn(
                    "message_id",
                    when(col("message_id").isNull(),
                         concat(lit("null_key_"), monotonically_increasing_id().cast("string")))
                    .otherwise(col("message_id"))
                )

            if has_error_records:
                error_types.add("VALIDATION_ERROR")
                logger.error(f"🚨 Batch {batch_id}: Messages failed parsing!")
                
                # Log sample error messages for debugging (lightweight)
                try:
                    sample_errors = error_df.select("error_type", "error_details").limit(3).collect()
                    for i, error_row in enumerate(sample_errors):
                        logger.error(f"  Error {i+1}: {error_row.error_type} - {error_row.error_details}")
                except Exception as e:
                    logger.error(f"  Could not collect error details: {str(e)}")
                
                process_error_records(error_df, batch_id, Config, 'api_logs')
                # Raise exception if validation fails
                raise Exception("Validation failed. Check error records for details.")

            if has_valid_records:
                # STEP 1: Transform raw API logs
                logger.info(f"🔄 Batch {batch_id}: Starting transformation")
                raw_api_logs = transform_raw_api_logs(valid_records, monitor, es_handler)
                
                # Enhanced check with problematic offset tracking
                has_transformed_records = raw_api_logs.take(1)
                logger.info(f"🔄 Batch {batch_id}: Transformation completed - {'Success' if has_transformed_records else 'No records produced'}")
                
                # Check if problematic offsets made it through transformation
                if has_transformed_records:
                    try:
                        transformed_problem_count = raw_api_logs.filter(
                            col("kafka_offset").isin([468, 469])
                        ).count()
                        if transformed_problem_count > 0:
                            logger.info(f"  ✅ Successfully transformed {transformed_problem_count} records from problematic offset range")
                        
                        # Log sample of transformed data structure for problematic offsets
                        if transformed_problem_count > 0:
                            try:
                                problem_transformed = raw_api_logs.filter(
                                    col("kafka_offset").isin([468, 469])
                                ).select(
                                    "kafka_offset", "request_id", "message_type", 
                                    "has_response", "request.source_ip", "response.status_code"
                                ).collect()
                                
                                for sample in problem_transformed:
                                    logger.info(f"    🔍 Transformed offset {sample.kafka_offset}: "
                                               f"ID={sample.request_id}, Type={sample.message_type}, "
                                               f"HasResponse={sample.has_response}, "
                                               f"SourceIP={getattr(sample, 'source_ip', 'N/A')}, "
                                               f"Status={getattr(sample, 'status_code', 'N/A')}")
                            except Exception as transform_debug_e:
                                logger.warning(f"    Could not log transformed record details: {str(transform_debug_e)}")
                                
                    except Exception as transform_check_e:
                        logger.warning(f"  Could not check transformed problematic offsets: {str(transform_check_e)}")
                
                if not has_transformed_records:
                    logger.warning(f"🚫 Batch {batch_id}: No valid API log records after transformation - batch skipped")
                    return
                
                # STEP 2: Transform for aggregation and aggregate API logs
                agg_ready_logs = transform_for_aggregation(raw_api_logs, monitor)
                aggregated_logs = aggregate_api_logs(agg_ready_logs, monitor)
                
                # STEP 3: Extract and transform SEVs from original valid records (before sevs was removed)
                raw_api_sevs = transform_api_sevs(valid_records, sevs_lookup_df, monitor, es_handler)
                
                # STEP 4: Aggregate SEVs (if any SEVs exist)
                if not raw_api_sevs.isEmpty():
                    agg_ready_sevs = transform_for_aggregation(raw_api_sevs, monitor)
                    aggregated_sevs = aggregate_api_sevs(agg_ready_sevs, monitor)
                else:
                    aggregated_sevs = raw_api_sevs  # Empty dataframe
                
                # STEP 5: Format and insert all data to Elasticsearch using parallel threads
                # Format API logs data
                raw_logs_formatted, agg_logs_formatted = format_api_logs_for_elasticsearch(
                    raw_api_logs, aggregated_logs, es_handler, monitor
                )
                
                # Format SEVs data (if any)
                raw_sevs_formatted = None
                agg_sevs_formatted = None
                if not raw_api_sevs.isEmpty():
                    raw_sevs_formatted, agg_sevs_formatted = format_api_sevs_for_elasticsearch(
                        raw_api_sevs, aggregated_sevs, es_handler, monitor
                    )
                
                # Log pre-insertion details for problematic offsets
                try:
                    pre_insert_problem_count = raw_logs_formatted.filter(
                        col("kafka_offset").isin([468, 469])
                    ).count()
                    if pre_insert_problem_count > 0:
                        logger.info(f"  💾 About to insert {pre_insert_problem_count} records from problematic offset range")
                        
                        # Log sample of data structure going to Elasticsearch
                        try:
                            pre_insert_samples = raw_logs_formatted.filter(
                                col("kafka_offset").isin([468, 469])
                            ).select(
                                "kafka_offset", "Log_ID", "request_id", "message_type", 
                                "has_response", "timestamp"
                            ).collect()
                            
                            for sample in pre_insert_samples:
                                logger.info(f"    💾 Pre-insert offset {sample.kafka_offset}: "
                                           f"LogID={sample.Log_ID}, RequestID={sample.request_id}, "
                                           f"Type={sample.message_type}, HasResponse={sample.has_response}")
                        except Exception as pre_insert_debug_e:
                            logger.warning(f"    Could not log pre-insert details: {str(pre_insert_debug_e)}")
                            
                except Exception as pre_insert_check_e:
                    logger.warning(f"  Could not check pre-insertion problematic offsets: {str(pre_insert_check_e)}")
                
                # Import and use centralized parallel insertion with enhanced error tracking
                try:
                    time_inserted_to_elasticsearch = insert_api_logs_data_parallel(
                        es_handler, raw_logs_formatted, agg_logs_formatted,
                        raw_sevs_formatted, agg_sevs_formatted, monitor, batch_id, Config
                    )
                    
                    # Log successful insertion of problematic offsets
                    logger.info(f"  ✅ Successfully inserted batch {batch_id} to Elasticsearch")
                    
                except Exception as insert_e:
                    logger.error(f"  ❌ Elasticsearch insertion failed for batch {batch_id}: {str(insert_e)}")
                    
                    # Log which offsets were being processed when insertion failed
                    try:
                        failed_offset_info = raw_logs_formatted.select("kafka_offset").agg(
                            {"kafka_offset": "min", "kafka_offset": "max"}
                        ).collect()[0]
                        failed_min = failed_offset_info["min(kafka_offset)"]
                        failed_max = failed_offset_info["max(kafka_offset)"]
                        logger.error(f"    🔢 Failed insertion covered offsets: {failed_min} to {failed_max}")
                        
                        # Check if problematic offsets were in failed batch
                        failed_problem_count = raw_logs_formatted.filter(
                            col("kafka_offset").isin([468, 469])
                        ).count()
                        if failed_problem_count > 0:
                            logger.error(f"    ⚠️  Failed batch included {failed_problem_count} records from problematic offset range (468-469)")
                            
                    except Exception as failed_debug_e:
                        logger.error(f"    Could not extract failed insertion details: {str(failed_debug_e)}")
                    
                    # Re-raise the exception to maintain error handling flow
                    raise insert_e

        logger.info(f"Batch {batch_id} completed in {time.time() - batch_start_time:.2f}s")
        
    except Exception as e:
        error_types.add("PROCESSING_ERROR")
        logger.error(f"Batch {batch_id} failed: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Log batch status with error before raising
        try:
            if df is not None and not df.isEmpty():
                batch_logger.log_batch(
                    job_type="api_logs",
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
        
        raise Exception(f"Batch {batch_id} processing failed: {str(e)}")
        
    else:
        # Only log success if no exceptions occurred
        try:
            if df is not None and not df.isEmpty():
                batch_logger.log_batch(
                    job_type="api_logs",
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
    """Main analysis function for unified API logs processing"""
    logger.info("Initializing unified API logs processing")
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
                batch_logger.create_logging_index_if_not_exists('api_logs')

                # Load lookup data (following ui_sevs pattern)
                m.mark_job("Load SEVs Lookup Data")
                sevs_lookup_df = ps_handler.load_by_query(
                    spark,
                    Config.API_SECURITY_EVENTS_LOOKUP_QUERY,
                    "sevs_lookup_df",
                    DataFrameStorageType.CACHED
                )

                if sevs_lookup_df is None:
                    logger.warning("Failed to load SEVs lookup table - SEV processing will be limited")
                    # Create empty dataframe with correct schema matching the actual lookup table
                    sevs_lookup_df = spark.createDataFrame([], "sev_id int, sev_name string, description string, severity string")
                else:
                    sevs_lookup_df = broadcast(sevs_lookup_df)

                # Initialize Kafka
                m.mark_job("Initialize Kafka")
                schema_validator = SchemaValidator()
                json_schema = get_schema_from_registry(
                    Config.GLUE_SCHEMA_REGISTRY_NAME,
                    Config.GLUE_SCHEMA_REGISTRY_REGION,
                    "api_logs_schema"
                )
                schema_validator.register_schema_in_validator("api_logs_schema", json_schema)

                kafka_handler = KafkaHandler(
                    Config.KAFKA_BOOTSTRAP_SERVERS,
                    Config.KAFKA_API_LOGS_TOPIC,
                    schema_validator,
                    skip_late_handling=True
                )
                
                # Create and start stream
                kafka_df = kafka_handler.create_kafka_stream(spark)
                query = kafka_df.writeStream \
                    .foreachBatch(lambda df, epoch_id: process_batch(
                        spark, df, epoch_id,
                        kafka_handler, es_handler, batch_logger, sevs_lookup_df
                    )) \
                    .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_API_LOGS) \
                    .trigger(processingTime=Config.PROCESSING_TIME) \
                    .start()

                logger.info("Unified API logs streaming started successfully")
                query.awaitTermination()

        except Exception as e:
            logger.error("Unified API logs processing failed", exc_info=True)
            raise

        finally:
            logger.info("Cleaning up resources...")
            if query is not None and query.isActive:
                try:
                    query.stop()
                    logger.info("Streaming query stopped")
                except Exception as e:
                    logger.error(f"Error stopping streaming query: {str(e)}")