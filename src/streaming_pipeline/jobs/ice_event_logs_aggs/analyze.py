import traceback
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame 
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, to_timestamp, window, explode, count, year, month, dayofmonth
)
from streaming_pipeline.shared.dal import KafkaHandler, PostgresHandler, DataFrameStorageType, IcebergHandler
from streaming_pipeline.shared.schema.schema_validator import SchemaValidator
from streaming_pipeline.shared.schema.schema_utils import process_error_records, get_schema_from_registry
from configs.config import Config

logger = logging.getLogger(__name__)

def process_current_events(df: DataFrame, broadcast_cars_signals: DataFrame, broadcast_vehicles_lookup: DataFrame ,iceberg_handler: IcebergHandler):
    watermarked_records = df.withColumn(
        "window",
        window("event_time", "1 hour")
    )

    enriched_df = watermarked_records.join(
        broadcast_cars_signals,
        col("DataName") == broadcast_cars_signals.signal_id,
        "left_outer"
    ).join(
        broadcast_vehicles_lookup,
        col("VinNumber") == broadcast_vehicles_lookup.vin
    )

    window_spec = Window.partitionBy(
        "window", 
        "VinNumber",
        "fleet_name", 
        "DataName", 
        "signal_name"
    )

    aggregated_df = enriched_df.withColumn(
        "count",
        count("*").over(window_spec)
    ).select(
        "window.start",
        "window.end",
        col("VinNumber").alias("VIN"),
        col("fleet_name"),
        col("DataName").alias("Signal_ID"),
        col("signal_name").alias("Signal_Name"),
        "count"
    ).distinct()

    final_df = aggregated_df.withColumn(
        "year", year("start")
    ).withColumn(
        "month", month("start")
    ).withColumn(
        "day", dayofmonth("start")
    )

    iceberg_handler.insert_aggregations(
        final_df,
        f"{Config.ICEBERG_DATABASE}.{Config.ICEBERG_EVENT_LOGS_AGGS_TABLE}",
        groupby_columns=["start", "VIN", "fleet_name", "Signal_ID", "Signal_Name"],
        agg_column="count"
    )

def process_batch(spark: SparkSession, df: DataFrame, batch_id: int, kafka_handler: KafkaHandler,
                 iceberg_handler: IcebergHandler, broadcast_cars_signals: DataFrame, broadcast_vehicles_lookup: DataFrame):
    if df.isEmpty():
        return

    try:
        valid_records, error_df = kafka_handler.parse_without_validation(spark, df, "logs_schema")
        
        if error_df.count() > 0:
            process_error_records(error_df, batch_id, Config, 'ice_event_logs_aggs')

        if valid_records.count() > 0:
            flattened_df = valid_records.select(
                col("VinNumber"),
                col("Campaign_ID"),
                explode("DataEntries").alias("DataEntry")
            ).select(
                col("VinNumber"),
                col("Campaign_ID"),
                col("DataEntry.DataName"),
                col("DataEntry.DataType"),
                col("DataEntry.DataValue"),
                to_timestamp(col("DataEntry.TimeStamp").cast("double") / 1000).alias("event_time")
            )

            process_current_events(flattened_df, broadcast_cars_signals, broadcast_vehicles_lookup, iceberg_handler)

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def analyze(spark: SparkSession):
    logger.info("Initializing Iceberg Event Logs Aggregations")

    with PostgresHandler(Config.POSTGRES_URL, Config.POSTGRES_PROPERTIES) as ps_handler:
        try:
            iceberg_handler = IcebergHandler(Config.ICEBERG_WAREHOUSE, Config.ICEBERG_CATALOG_NAME)

            cars_signals_df = ps_handler.load_by_query(
                spark,
                """
                SELECT
                    signal_id,
                    signal_name,
                    signal_unit,
                    component,
                    min_value,
                    max_value,
                    signal_fully_qualified_name
                FROM cars_signals
                """,
                "cars_signals_df",
                DataFrameStorageType.BROADCASTED
            )

                        # Load lookup tables once at startup
            query_vin = """
                SELECT 
                    v.vin, 
                    vm.model_name AS vehicle_model, 
                    fc."Fleet Name" AS fleet_name
                FROM 
                    vehicle v
                LEFT JOIN 
                    vehicle_model vm ON v.vehicle_model_id = vm.id
                LEFT JOIN 
                    fleet_cars fc ON v.vin = fc."VIN Number"
            """

            vehicles_lookup_df = ps_handler.load_by_query(
                spark, 
                query_vin, 
                "vehicles_lookup_df",
                DataFrameStorageType.BROADCASTED
            )

            schema_validator = SchemaValidator()
            schema_validator.register_schema_in_validator(
                "logs_schema",
                get_schema_from_registry(
                    Config.GLUE_SCHEMA_REGISTRY_NAME,
                    Config.GLUE_SCHEMA_REGISTRY_REGION,
                    "logs_schema"
                )
            )

            kafka_handler = KafkaHandler(
                Config.KAFKA_BOOTSTRAP_SERVERS,
                Config.KAFKA_LOGS_TOPIC,
                schema_validator
            )

            kafka_df = kafka_handler.create_kafka_stream(spark)
            
            query = kafka_df.writeStream \
                .foreachBatch(lambda df, epoch_id: process_batch(
                    spark, df, epoch_id, kafka_handler, iceberg_handler,
                    cars_signals_df, vehicles_lookup_df
                )) \
                .option("checkpointLocation", Config.S3_CHECKPOINT_LOCATION_LOGS_AGGS) \
                .trigger(processingTime=Config.PROCESSING_TIME) \
                .start()

            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in Iceberg Event Logs Aggregations: {str(e)}")
            logger.error(traceback.format_exc())
            if 'query' in locals() and query.isActive:
                query.stop()
            raise