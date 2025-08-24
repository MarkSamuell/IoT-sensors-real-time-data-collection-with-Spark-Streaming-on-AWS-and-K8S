import traceback
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, to_timestamp, date_format, window, explode, count, broadcast
)
from streaming_pipeline.shared.dal import (
    ElasticsearchHandler, KafkaHandler, PostgresHandler, DataFrameStorageType
)
from streaming_pipeline.shared.schema.schema_utils import (
    get_schema_from_registry, process_error_records, create_error_record
)
from streaming_pipeline.shared.schema.schema_validator import SchemaValidator
from streaming_pipeline.shared.batch_logger import BatchLogger
from streaming_pipeline.shared.acks_handler import AckHandler
from streaming_pipeline.shared.job_monitor import SparkJobMonitor
from configs.config import Config
from typing import List
import time

logger = logging.getLogger(__name__)

def process_current_events(df: DataFrame, broadcast_cars_signals: DataFrame,
                         es_handler: ElasticsearchHandler, monitor: SparkJobMonitor) -> str:
    try:
        with monitor as m:
            
            watermarked_df = df \
                .withColumn("window", window("event_time", "1 hour"))

            m.mark_job("Join Lookup Data")
            enriched_df = watermarked_df.join(
                broadcast_cars_signals,
                col("DataName") == broadcast_cars_signals.signal_id,
                "left_outer"
            ).drop(broadcast_cars_signals.signal_id)

            m.mark_job("Window Aggregations")
            # Group by the columns we want to aggregate
            aggregated_df = enriched_df.groupBy(
                "window",
                col("VinNumber").alias("VIN"),
                col("DataName").alias("Signal_ID"),
                col("db_signal_name").alias("Signal_Name")
            ).agg(
                count("*").alias("count")
            )

            m.mark_job("Format for ES")
            es_ready_df = es_handler.add_log_id(
                aggregated_df.select(
                    date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
                    date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
                    col("VIN"),
                    col("Signal_ID"),
                    col("Signal_Name"),
                    col("count")
                ),
                "es_id",
                ["VIN", "window_start", "Signal_ID", "Signal_Name"]
            )

            m.mark_job("Insert to ES")
            es_handler.insert_aggregations(es_ready_df, Config.ELASTICSEARCH_EVENT_LOGS_AGGS_INDEX)
            
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        logger.error(f"Error processing current events: {str(e)}")
        raise

def process_batch(spark: SparkSession, df: DataFrame, batch_id: int, kafka_handler: KafkaHandler,
                  ps_handler: PostgresHandler, es_handler: ElasticsearchHandler,
                  broadcast_cars_signals: DataFrame, batch_logger: BatchLogger):
    
    monitor = SparkJobMonitor(spark)
    error_types = set()
    time_received_from_kafka = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    time_inserted_to_elasticsearch = None
    batch_start_time = time.time()

    try:
        with monitor as m:
            if df.isEmpty():
                return

            m.mark_job(f"Batch {batch_id}: Parse Messages")
            valid_records, error_df = kafka_handler.parse_without_validation(
                spark, df, "logs_schema"
            )

            if error_df.take(1):
                error_types.add("VALIDATION_ERROR")
                process_error_records(error_df, batch_id, Config, 'event_logs_aggs')

            if valid_records.take(1):
                # message_keys = valid_records.select("message_id").rdd.map(lambda x: x[0]).collect()
                
                flattened_df = valid_records.select(
                    col("message_id"),
                    col("VinNumber"),
                    col("Campaign_ID"),
                    explode("DataEntries").alias("DataEntry")
                ).select(
                    col("message_id"),
                    col("VinNumber"),
                    col("Campaign_ID"),
                    col("DataEntry.DataName"),
                    col("DataEntry.DataType"),
                    col("DataEntry.DataValue"),
                    col("DataEntry.TimeStamp"),
                    to_timestamp(col("DataEntry.TimeStamp").cast("double") / 1000).alias("event_time")
                )

                time_inserted_to_elasticsearch = process_current_events(
                    flattened_df, broadcast_cars_signals, es_handler, 
                    monitor
                )

        logger.info(f"Batch {batch_id} completed in {time.time() - batch_start_time:.2f}s")

    except Exception as e:
        error_types.add("PROCESSING_ERROR")
        logger.error(f"Batch {batch_id} failed: {str(e)}")
        raise

    finally:
        batch_logger.log_batch(
            job_type="event_logs_aggs",
            step_id=str(spark.sparkContext.applicationId),
            batch_number=batch_id,
            kafka_df=df,
            kafka_timestamp=time_received_from_kafka,
            time_received_from_kafka=time_received_from_kafka,
            time_inserted_to_elasticsearch=time_inserted_to_elasticsearch,
            is_error=bool(error_types),
            error_types=list(error_types) if error_types else None
        )

def analyze(spark: SparkSession):
    logger.info("Initializing Event Logs Aggregations analysis")
    monitor = SparkJobMonitor(spark)

    with PostgresHandler(Config.POSTGRES_URL, Config.POSTGRES_PROPERTIES) as ps_handler:
        try:
            with monitor as m:
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
                batch_logger.create_logging_index_if_not_exists('event_logs_aggs')

                m.mark_job("Load Lookup Data")
                cars_signals_df = ps_handler.load_by_query(
                    spark,
                    """SELECT
                        business_id as signal_id,
                        name as db_signal_name,
                        unit as signal_unit,
                        CASE 
                                WHEN min::text = 'NaN' THEN NULL 
                                ELSE min 
                            END as min_value,
                        CASE 
                                WHEN max::text = 'NaN' THEN NULL 
                                ELSE max 
                            END as max_value,
                        signal_fully_qualified_name
                    FROM
                        signal""",
                    "cars_signals_df",
                    DataFrameStorageType.CACHED
                )
                if cars_signals_df is None:
                    raise ValueError("Failed to load cars_signals lookup data")
                
                cars_signals_df = broadcast(cars_signals_df)

                m.mark_job("Initialize Schema")
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

                kafka_df = kafka_handler.create_kafka_stream(spark)
                query = kafka_df.writeStream \
                    .foreachBatch(lambda df, epoch_id: process_batch(
                        spark, df, epoch_id, kafka_handler, ps_handler,
                        es_handler, cars_signals_df, batch_logger
                    )) \
                    .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_LOGS_AGGS) \
                    .trigger(processingTime=Config.PROCESSING_TIME) \
                    .start()

                logger.info("Event Logs Aggregations Started Successfully")
                query.awaitTermination()

        except Exception as e:
            logger.error("Stream processing failed", exc_info=True)
            if 'query' in locals() and query.isActive:
                query.stop()
            raise